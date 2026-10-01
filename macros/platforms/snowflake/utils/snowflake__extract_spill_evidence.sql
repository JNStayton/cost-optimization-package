{% macro snowflake__extract_spill_evidence() %}

  {#--
    Post-hook on fct_snowflake__warehouse_performance_recommendations. For each spilling
    table it lists, samples the most recent completed spilling query of each query shape
    (up to 5 shapes per table) from the last 14 days, the GET_QUERY_OPERATOR_STATS
    retention, and records where the query's time went in
    int_snowflake__query_spill_evidence. Queries already recorded are skipped.

    Per query: execution time (T), and the time its spilling operators were blocked on
    disk (S = T x the sum of their local and remote disk I/O shares). Only operators with
    spilling statistics count: other disk I/O (cache reads, scans) isn't spill.

    Each query is read in a Snowflake Scripting block, so one the role can't analyze
    (it needs to own the query or hold MONITOR on the warehouse) is recorded as
    skipped instead of failing the build. A coverage line is logged per table.
    Enterprise edition only: queries are attributed to tables through ACCESS_HISTORY.
  --#}

  {% if execute and target.type == 'snowflake' and var('snowflake_enterprise_edition', true) %}

    {% set evidence_table = ref('int_snowflake__query_spill_evidence') %}
    {% set query_history_table = ref('int_snowflake__query_history') %}
    {% set table_access = ref('int_snowflake__query_table_access') %}

    {% set queries_sql %}
      select table_fqn, query_id, query_parameterized_hash, query_start_time,
             warehouse_name, warehouse_size, execution_time_s
      from (
          select
              p.table_fqn,
              qh.query_id,
              qh.query_parameterized_hash,
              qh.query_start_time,
              qh.warehouse_name,
              qh.warehouse_size,
              coalesce(qh.execution_time_ms, 0) / 1000.0 as execution_time_s,
              row_number() over (
                  partition by p.table_fqn, qh.query_parameterized_hash
                  order by qh.query_start_time desc
              ) as shape_rank
          from {{ this }} as p
          inner join {{ table_access }} as qta
              on upper(qta.table_database) || '.' || upper(qta.table_schema) || '.' || upper(qta.table_name) = p.table_fqn
          inner join {{ query_history_table }} as qh
              on qh.query_id = qta.query_id
          where p.table_fqn is not null
            and qh.query_start_time >= dateadd(day, -14, current_timestamp())
            and (qh.bytes_spilled_local > 0 or qh.bytes_spilled_remote > 0)
            and qh.execution_status = 'SUCCESS'
            and md5(qh.query_id || '|' || p.table_fqn) not in (
                select spill_evidence_key from {{ evidence_table }} where spill_evidence_key is not null)
      )
      where shape_rank = 1
      qualify row_number() over (partition by table_fqn order by query_start_time desc) <= 5
      order by table_fqn, query_start_time desc
    {% endset %}

    {% set queries = run_query(queries_sql) %}

    {% if queries and queries.rows | length > 0 %}
      {% set coverage = {} %}

      {% for q in queries %}
        {% set table_fqn = q['TABLE_FQN'] %}
        {% set qid = q['QUERY_ID'] %}
        {% set key_sql = "md5('" ~ qid ~ "' || '|' || '" ~ table_fqn ~ "')" %}
        {% set common_cols %}
            {{ key_sql }}                                         as spill_evidence_key,
            '{{ qid }}'                                           as query_id,
            '{{ table_fqn }}'                                     as table_fqn,
            '{{ q['QUERY_PARAMETERIZED_HASH'] }}'                 as query_parameterized_hash,
            '{{ q['QUERY_START_TIME'] }}'::timestamp_ltz          as query_start_time,
            '{{ q['WAREHOUSE_NAME'] }}'                           as warehouse_name,
            '{{ q['WAREHOUSE_SIZE'] }}'                           as warehouse_size,
            {{ q['EXECUTION_TIME_S'] }}::float                    as execution_time_s
        {% endset %}

        {% set merge_sql %}
          execute immediate $$
          begin
            merge into {{ evidence_table }} as target
            using (
                select
                    {{ common_cols }},
                    count_if(operator_statistics:spilling is not null)               as spilling_operator_count,
                    coalesce(sum(iff(operator_statistics:spilling is not null,
                        execution_time_breakdown:overall_percentage::float, 0)), 0)  as spilling_operator_share,
                    coalesce(sum(iff(operator_statistics:spilling is not null,
                        coalesce(execution_time_breakdown:local_disk_io::float, 0)
                        + coalesce(execution_time_breakdown:remote_disk_io::float, 0), 0)), 0)
                                                                                     as blocked_on_disk_share,
                    {{ q['EXECUTION_TIME_S'] }}::float * coalesce(sum(iff(operator_statistics:spilling is not null,
                        coalesce(execution_time_breakdown:local_disk_io::float, 0)
                        + coalesce(execution_time_breakdown:remote_disk_io::float, 0), 0)), 0)
                                                                                     as spill_blocked_s,
                    coalesce(sum(operator_statistics:spilling:bytes_spilled_local_storage::bigint), 0)
                                                                                     as bytes_spilled_local_ops,
                    coalesce(sum(operator_statistics:spilling:bytes_spilled_remote_storage::bigint), 0)
                                                                                     as bytes_spilled_remote_ops,
                    'ok'                                                             as evidence_status,
                    null::varchar                                                    as skip_reason,
                    current_timestamp()                                              as analyzed_at
                from table(get_query_operator_stats('{{ qid }}'))
            ) as source
            on target.spill_evidence_key = source.spill_evidence_key
            when not matched then insert
                (spill_evidence_key, query_id, table_fqn, query_parameterized_hash, query_start_time,
                 warehouse_name, warehouse_size, execution_time_s, spilling_operator_count,
                 spilling_operator_share, blocked_on_disk_share, spill_blocked_s,
                 bytes_spilled_local_ops, bytes_spilled_remote_ops, evidence_status, skip_reason, analyzed_at)
            values
                (source.spill_evidence_key, source.query_id, source.table_fqn, source.query_parameterized_hash,
                 source.query_start_time, source.warehouse_name, source.warehouse_size, source.execution_time_s,
                 source.spilling_operator_count, source.spilling_operator_share, source.blocked_on_disk_share,
                 source.spill_blocked_s, source.bytes_spilled_local_ops, source.bytes_spilled_remote_ops,
                 source.evidence_status, source.skip_reason, source.analyzed_at);
            return null;
          exception
            when other then
              return sqlerrm;
          end;
          $$
        {% endset %}

        {% set outcome = run_query(merge_sql).columns[0].values()[0] %}
        {% set counts = coverage.get(table_fqn, [0, 0]) %}
        {% if outcome is none %}
          {% do coverage.update({table_fqn: [counts[0] + 1, counts[1]]}) %}
        {% else %}
          {% do coverage.update({table_fqn: [counts[0], counts[1] + 1]}) %}
          {% set skip_sql %}
            merge into {{ evidence_table }} as target
            using (
                select
                    {{ common_cols }},
                    null::int as spilling_operator_count, null::float as spilling_operator_share,
                    null::float as blocked_on_disk_share, null::float as spill_blocked_s,
                    null::bigint as bytes_spilled_local_ops, null::bigint as bytes_spilled_remote_ops,
                    'skipped' as evidence_status,
                    $${{ outcome[:1000] | replace('$$', '') }}$$ as skip_reason,
                    current_timestamp() as analyzed_at
            ) as source
            on target.spill_evidence_key = source.spill_evidence_key
            when not matched then insert
                (spill_evidence_key, query_id, table_fqn, query_parameterized_hash, query_start_time,
                 warehouse_name, warehouse_size, execution_time_s, spilling_operator_count,
                 spilling_operator_share, blocked_on_disk_share, spill_blocked_s,
                 bytes_spilled_local_ops, bytes_spilled_remote_ops, evidence_status, skip_reason, analyzed_at)
            values
                (source.spill_evidence_key, source.query_id, source.table_fqn, source.query_parameterized_hash,
                 source.query_start_time, source.warehouse_name, source.warehouse_size, source.execution_time_s,
                 source.spilling_operator_count, source.spilling_operator_share, source.blocked_on_disk_share,
                 source.spill_blocked_s, source.bytes_spilled_local_ops, source.bytes_spilled_remote_ops,
                 source.evidence_status, source.skip_reason, source.analyzed_at)
          {% endset %}
          {% do run_query(skip_sql) %}
          {{ log("extract_spill_evidence: skipped query " ~ qid ~ " for " ~ table_fqn ~ " (" ~ outcome ~ ")", info=true) }}
        {% endif %}
      {% endfor %}

      {% for table_fqn, counts in coverage.items() %}
        {{ log("extract_spill_evidence: " ~ table_fqn ~ ": analyzed " ~ counts[0] ~ " of "
               ~ (counts[0] + counts[1]) ~ " sampled spilling queries (" ~ counts[1] ~ " skipped)", info=true) }}
      {% endfor %}
    {% else %}
      {{ log("extract_spill_evidence: no new spilling queries to analyze.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
