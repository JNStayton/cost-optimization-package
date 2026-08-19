{% macro extract_operator_evidence() %}

  {#--
    Unified operator evidence extraction. Populates int_snowflake__query_operator_evidence
    with TableScan (partition pruning), Filter (WHERE columns), and Join (equality columns)
    evidence from GET_QUERY_OPERATOR_STATS.

    Runs as a post-hook on fct_snowflake__table_clustering_candidates. Processes
    representative queries for the top N candidate tables.

    Query discovery:
      - Enterprise: exact FQN from ACCESS_HISTORY
      - Standard: exact FQN text matching (contains() — no bare ILIKE)
      - Both: prefer non-cached executions, exclude tests/package queries

    Operator extraction (one call per query):
      - TableScan: partitions_scanned, partitions_total, bytes_scanned, table_name
      - Filter: filter_condition matched against known table columns
      - Join: equality_join_condition matched against known table columns

    Variables:
      clustering_key_operator_analysis_table_limit (default 10)
      clustering_key_operator_queries_per_table    (default 20)
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set is_enterprise = var('snowflake_enterprise_edition', true) %}
    {% set table_limit = var('clustering_key_operator_analysis_table_limit', 10) %}
    {% set queries_per_table = var('clustering_key_operator_queries_per_table', 20) %}

    {% set candidates_table = ref('fct_snowflake__table_clustering_candidates') %}
    {% set evidence_table = ref('int_snowflake__query_operator_evidence') %}
    {% set table_columns_ref = ref('int_snowflake__table_columns') %}
    {% set query_history_table = ref('int_snowflake__query_history') %}
    {% set relations_table = ref('int_dbt__relations') %}
    {# Enterprise-only: ACCESS_HISTORY-derived column access. String ref to avoid parse error on Standard. #}
    {% set col_access_table = evidence_table.database ~ '.' ~ evidence_table.schema ~ '.int_snowflake__column_query_access' %}

    {{ log("extract_operator_evidence: fetching top " ~ table_limit ~ " candidates...", info=true) }}

    {# Step 1: Get top candidate tables #}
    {% set candidates_sql %}
      select
          table_fqn,
          database_name,
          schema_name,
          table_name
      from {{ candidates_table }}
      where is_candidate = true
          and snapshot_date = current_date()
      qualify row_number() over (order by score desc) <= {{ table_limit }}
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for cand_row in candidates %}

        {% set table_fqn = cand_row['TABLE_FQN'] %}

        {{ log("extract_operator_evidence: analyzing " ~ table_fqn, info=true) }}

        {# Step 1b: Find direct downstream children of this candidate #}
        {% set children_sql %}
          select table_fqn
          from {{ relations_table }}
          where array_contains('{{ table_fqn }}'::variant, parent_models)
        {% endset %}

        {% set children_result = run_query(children_sql) %}
        {% set child_fqns = children_result.columns[0].values() if children_result and children_result.rows | length > 0 else [] %}

        {# Step 2: Discover representative queries #}
        {% if is_enterprise %}
        {% set queries_sql %}
          select qh.query_id, qh.query_start_time, qh.query_parameterized_hash
          from (
              select
                  ca.query_id,
                  ca.query_start_time,
                  qh.query_parameterized_hash,
                  row_number() over (
                      partition by qh.query_parameterized_hash
                      order by
                          iff(qh.bytes_scanned > 0 or qh.partitions_scanned > 0, 0, 1),
                          ca.query_start_time desc
                  ) as rn
              from {{ col_access_table }} as ca
              inner join {{ query_history_table }} as qh
                  on ca.query_id = qh.query_id
              where ca.table_fqn in ('{{ table_fqn }}'{% for child_fqn in child_fqns %}, '{{ child_fqn }}'{% endfor %})
                  and ca.query_start_time >= dateadd(day, -14, current_timestamp())
                  and qh.query_type = 'SELECT'
          ) as qh
          where rn = 1
          order by query_start_time desc
          limit {{ queries_per_table }}
        {% endset %}
        {% else %}
        {# Standard: exact FQN text matching #}
        {% set queries_sql %}
          select query_id, query_start_time, query_parameterized_hash
          from (
              select
                  qh.query_id,
                  qh.query_start_time,
                  qh.query_parameterized_hash,
                  row_number() over (
                      partition by qh.query_parameterized_hash
                      order by
                          iff(qh.bytes_scanned > 0 or qh.partitions_scanned > 0, 0, 1),
                          qh.query_start_time desc
                  ) as rn
              from {{ query_history_table }} as qh
              where qh.query_type = 'SELECT'
                  and qh.query_start_time >= dateadd(day, -14, current_timestamp())
                  and (
                    contains(upper(replace(qh.query_text, '"', '')), '{{ table_fqn }}')
                    {% for child_fqn in child_fqns %}
                    or contains(upper(replace(qh.query_text, '"', '')), '{{ child_fqn }}')
                    {% endfor %}
                  )
                  and qh.query_text not ilike '%dbt_internal_test%'
                  and qh.query_text not ilike '%dbt_utils%'
                  and qh.query_text not ilike '%int_snowflake__query_operator%'
          )
          where rn = 1
          order by query_start_time desc
          limit {{ queries_per_table }}
        {% endset %}
        {% endif %}

        {% set queries_result = run_query(queries_sql) %}

        {% if queries_result and queries_result.rows | length > 0 %}

          {{ log("extract_operator_evidence: processing " ~ (queries_result.rows | length) ~ " queries for " ~ table_fqn, info=true) }}

          {# Step 3: For each query, extract ALL operator evidence in one MERGE #}
          {% for q_row in queries_result %}

            {% set qid = q_row['QUERY_ID'] %}
            {% set qstart = q_row['QUERY_START_TIME'] %}
            {% set qhash = q_row['QUERY_PARAMETERIZED_HASH'] %}

            {% set merge_sql %}
              merge into {{ evidence_table }} as target
              using (
                  -- TableScan evidence: exact per-table partition pruning
                  select
                      md5('{{ qid }}' || '|' || 'TableScan' || '|' || operator_attributes:table_name::string) as operator_evidence_key,
                      '{{ qid }}' as query_id,
                      upper(trim(replace(operator_attributes:table_name::string, '"', ''))) as table_fqn,
                      'TableScan' as operator_type,
                      cast(null as varchar) as column_name,
                      operator_statistics:pruning:partitions_scanned::int as partitions_scanned,
                      operator_statistics:pruning:partitions_total::int as partitions_total,
                      operator_statistics:io:bytes_scanned::bigint as bytes_scanned,
                      cast(null as varchar) as condition_text,
                      '{{ qhash }}' as query_parameterized_hash,
                      '{{ qstart }}'::timestamp_ntz as query_start_time,
                      cast('{{ qstart }}'::timestamp_ntz as date) as access_date
                  from table(get_query_operator_stats('{{ qid }}'))
                  where operator_type = 'TableScan'
                      and operator_attributes:table_name::string is not null
                      and upper(trim(replace(operator_attributes:table_name::string, '"', ''))) = '{{ table_fqn }}'

                  union all

                  -- Filter/Join evidence: column usage matched against candidate table columns
                  select
                      md5('{{ qid }}' || '|' || ops.operator_type || '|' || '{{ table_fqn }}' || '|' || cols.column_name) as operator_evidence_key,
                      '{{ qid }}' as query_id,
                      '{{ table_fqn }}' as table_fqn,
                      ops.operator_type,
                      cols.column_name,
                      cast(null as int) as partitions_scanned,
                      cast(null as int) as partitions_total,
                      cast(null as bigint) as bytes_scanned,
                      ops.condition_text,
                      '{{ qhash }}' as query_parameterized_hash,
                      '{{ qstart }}'::timestamp_ntz as query_start_time,
                      cast('{{ qstart }}'::timestamp_ntz as date) as access_date
                  from (
                      select
                          operator_type,
                          case
                              when operator_type = 'Filter'
                                  then operator_attributes:filter_condition::string
                              when operator_type = 'Join'
                                  then operator_attributes:equality_join_condition::string
                          end as condition_text
                      from table(get_query_operator_stats('{{ qid }}'))
                      where operator_type in ('Filter', 'Join')
                  ) as ops
                  cross join (
                      select upper(column_name) as column_name
                      from {{ table_columns_ref }}
                      where table_fqn = '{{ table_fqn }}'
                  ) as cols
                  where ops.condition_text ilike '%' || cols.column_name || '%'
              ) as source
              on target.operator_evidence_key = source.operator_evidence_key
              when not matched then insert
                  (operator_evidence_key, query_id, table_fqn, operator_type, column_name,
                   partitions_scanned, partitions_total, bytes_scanned, condition_text,
                   query_parameterized_hash, query_start_time, access_date)
              values
                  (source.operator_evidence_key, source.query_id, source.table_fqn, source.operator_type,
                   source.column_name, source.partitions_scanned, source.partitions_total, source.bytes_scanned,
                   source.condition_text, source.query_parameterized_hash, source.query_start_time, source.access_date)
            {% endset %}

            {% do run_query(merge_sql) %}

          {% endfor %}

          {{ log("extract_operator_evidence: completed " ~ table_fqn ~ " (" ~ (queries_result.rows | length) ~ " queries)", info=true) }}

        {% else %}
          {{ log("extract_operator_evidence: no recent queries for " ~ table_fqn ~ ", skipping.", info=true) }}
        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("extract_operator_evidence: no candidates found for today, skipping.", info=true) }}
    {% endif %}

    {# Step 4: Backfill consumption metrics on table_clustering_candidates #}
    {{ log("extract_operator_evidence: backfilling consumption metrics...", info=true) }}

    {% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
    {% set workload_class_table = ref('int_snowflake__query_workload_class') %}

    {% set backfill_sql %}
      update {{ candidates_table }} as tgt
      set
          consumption_query_count = src.consumption_query_count,
          consumption_query_shape_count = src.consumption_query_shape_count,
          consumption_scan_ratio_pct = src.consumption_scan_ratio_pct,
          transformation_scan_ratio_pct = src.transformation_scan_ratio_pct,
          transformation_query_count = src.transformation_query_count,
          recommendation_status = src.recommendation_status
      from (
          select
              oe.table_fqn,
              count(distinct case when wc.workload_class = 'consumption' then oe.query_id end) as consumption_query_count,
              count(distinct case when wc.workload_class = 'consumption' then oe.query_parameterized_hash end) as consumption_query_shape_count,
              case
                  when sum(case when wc.workload_class = 'consumption' then oe.partitions_total else 0 end) > 0
                      then round(
                          sum(case when wc.workload_class = 'consumption' then oe.partitions_scanned else 0 end)::float
                          / sum(case when wc.workload_class = 'consumption' then oe.partitions_total else 0 end) * 100, 2)
                  else null
              end as consumption_scan_ratio_pct,
              case
                  when sum(case when wc.workload_class = 'dbt_model_build' then oe.partitions_total else 0 end) > 0
                      then round(
                          sum(case when wc.workload_class = 'dbt_model_build' then oe.partitions_scanned else 0 end)::float
                          / sum(case when wc.workload_class = 'dbt_model_build' then oe.partitions_total else 0 end) * 100, 2)
                  else null
              end as transformation_scan_ratio_pct,
              count(distinct case when wc.workload_class = 'dbt_model_build' then oe.query_id end) as transformation_query_count,
              case
                  when count(distinct case when wc.workload_class = 'consumption' then oe.query_id end) = 0
                      then 'insufficient_evidence'
                  when sum(case when wc.workload_class = 'consumption' then oe.partitions_scanned else 0 end)::float
                      / nullif(sum(case when wc.workload_class = 'consumption' then oe.partitions_total else 0 end), 0) > 0.5
                      then 'evaluate_clustering'
                  when sum(case when wc.workload_class = 'consumption' then oe.partitions_scanned else 0 end)::float
                      / nullif(sum(case when wc.workload_class = 'consumption' then oe.partitions_total else 0 end), 0) <= 0.5
                      then 'healthy'
                  else 'investigate'
              end as recommendation_status
          from {{ evidence_table }} as oe
          inner join {{ workload_class_table }} as wc
              on oe.query_id = wc.query_id
          where oe.operator_type = 'TableScan'
            and oe.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
          group by oe.table_fqn
      ) as src
      where tgt.table_fqn = src.table_fqn
        and tgt.snapshot_date = current_date()
    {% endset %}

    {% do run_query(backfill_sql) %}

    {{ log("extract_operator_evidence: backfill complete.", info=true) }}

  {% endif %}

{% endmacro %}
