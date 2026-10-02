{% macro snowflake__probe_view_recompute() %}

  {#--
    Post-hook on int_snowflake__view_probe. Probes up to
    table_materialization_view_probe_limit views per run that feed a table through a
    view chain and haven't been probed in table_materialization_view_probe_refresh_days:
    deepest chains first (their read durations are least reliable), then views feeding
    the most tables.

    Each probe runs `select hash_agg(*) from <view>` with the result cache off, in a
    Snowflake Scripting block so a view that fails (dropped, or not readable by the
    package's role) is recorded as failed instead of failing the build. The execution
    time comes from INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION, which has no latency.

    Never probed: this package's own views, views outside dbt_monitored_projects, and
    views in schemas left out by dbt_excluded_schemas.
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set probe_limit = var('table_materialization_view_probe_limit', 10) | int %}
    {% set refresh_days = var('table_materialization_view_probe_refresh_days', 7) | int %}
    {% set monitored_projects = var('dbt_monitored_projects', []) %}
    {% if monitored_projects | length == 0 %}
      {% set monitored_projects = [project_name] %}
    {% endif %}

    {% if probe_limit <= 0 %}
      {{ log("probe_view_recompute: table_materialization_view_probe_limit is 0, skipping.", info=true) }}
    {% else %}

      {% set candidates_sql %}
        select
            p.upstream_fqn          as view_fqn,
            max(p.path_length)      as chain_depth,
            count(distinct p.table_fqn) as downstream_table_count
        from {{ ref('int_snowflake__view_chain_pairs') }} as p
        where p.upstream_materialized = 'view'
          and p.upstream_package_name != 'dbt_cost_optimization'
          {% if not (monitored_projects | length == 1 and monitored_projects[0] == '*') %}
          and p.upstream_package_name in (
              {%- for proj in monitored_projects -%}'{{ proj }}'{% if not loop.last %}, {% endif %}{%- endfor -%})
          {% endif %}
          and not {{ relation_is_excluded("split_part(p.upstream_fqn, '.', 2)", 'null') }}
          and p.upstream_fqn not in (
              select view_fqn from {{ this }}
              where view_fqn is not null
                and probed_at >= dateadd(day, -{{ refresh_days }}, current_timestamp())
          )
        group by p.upstream_fqn
        order by chain_depth desc, downstream_table_count desc, view_fqn
        limit {{ probe_limit }}
      {% endset %}

      {% set candidates = run_query(candidates_sql) %}

      {% if candidates and candidates.rows | length > 0 %}
        {% do run_query("alter session set use_cached_result = false") %}

        {% for row in candidates %}
          {% set view_fqn = row['VIEW_FQN'] %}
          {{ log("probe_view_recompute: probing " ~ view_fqn, info=true) }}

          {% set probe_sql %}
            execute immediate $$
            begin
                select hash_agg(*) from {{ view_fqn }};
                return 'ok:' || sqlid;
            exception
                when other then
                    return 'failed:' || sqlerrm;
            end;
            $$
          {% endset %}
          {% set outcome = run_query(probe_sql).columns[0].values()[0] %}
          {% set succeeded = outcome.startswith('ok:') %}
          {% set detail = outcome[3:] if succeeded else outcome[7:] %}

          {% set merge_sql %}
            merge into {{ this }} as target
            using (
                {% if succeeded %}
                select
                    '{{ view_fqn }}'                 as view_fqn,
                    query_id                         as probe_query_id,
                    'ok'                             as probe_status,
                    null::varchar                    as probe_error,
                    execution_time                   as execution_time_ms,
                    total_elapsed_time               as total_elapsed_time_ms,
                    warehouse_name,
                    warehouse_size,
                    current_timestamp()              as probed_at
                from table({{ this.database }}.information_schema.query_history_by_session(result_limit => 1000))
                where query_id = '{{ detail }}'
                {% else %}
                select
                    '{{ view_fqn }}'                 as view_fqn,
                    null::varchar                    as probe_query_id,
                    'failed'                         as probe_status,
                    $${{ detail[:1000] | replace('$$', '') }}$$ as probe_error,
                    null::bigint                     as execution_time_ms,
                    null::bigint                     as total_elapsed_time_ms,
                    null::varchar                    as warehouse_name,
                    null::varchar                    as warehouse_size,
                    current_timestamp()              as probed_at
                {% endif %}
            ) as source
            on target.view_fqn = source.view_fqn
            when matched then update set
                probe_query_id        = source.probe_query_id,
                probe_status          = source.probe_status,
                probe_error           = source.probe_error,
                execution_time_ms     = source.execution_time_ms,
                total_elapsed_time_ms = source.total_elapsed_time_ms,
                warehouse_name        = source.warehouse_name,
                warehouse_size        = source.warehouse_size,
                probed_at             = source.probed_at
            when not matched then insert
                (view_fqn, probe_query_id, probe_status, probe_error, execution_time_ms,
                 total_elapsed_time_ms, warehouse_name, warehouse_size, probed_at)
            values
                (source.view_fqn, source.probe_query_id, source.probe_status, source.probe_error,
                 source.execution_time_ms, source.total_elapsed_time_ms, source.warehouse_name,
                 source.warehouse_size, source.probed_at)
          {% endset %}
          {% do run_query(merge_sql) %}
          {% if not succeeded %}
            {{ log("probe_view_recompute: probe of " ~ view_fqn ~ " failed: " ~ detail, info=true) }}
          {% endif %}
        {% endfor %}

        {% do run_query("alter session unset use_cached_result") %}
      {% else %}
        {{ log("probe_view_recompute: no views due for a probe.", info=true) }}
      {% endif %}

    {% endif %}
  {% endif %}

{% endmacro %}
