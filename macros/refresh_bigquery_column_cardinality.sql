{% macro refresh_bigquery_column_cardinality() %}

  {#--
    Populates int_bigquery__column_cardinality with APPROX_COUNT_DISTINCT values for
    the top N clustering candidates identified by fct_bigquery__table_clustering_candidates.

    Runs automatically as a post-hook on fct_bigquery__table_clustering_candidates,
    so it fires after candidates are built and before fct_bigquery__clustering_key_candidates
    reads from the cardinality table.

    Steps:
      1. Query top N candidates from the already-built candidates fact model.
      2. For each candidate, look up which columns have actual query access in the
         lookback window from int_bigquery__column_query_stats (when
         use_query_text_attribution = true) — this is the cost-discipline pre-filter
         that avoids scanning every column of every large table.
      3. Run APPROX_COUNT_DISTINCT for all pre-filtered columns in a single scan per
         table (one SELECT computing every column's APPROX_COUNT_DISTINCT, unpivoted
         into rows) rather than one scan per column — BigQuery's on-demand pricing
         has a minimum-bytes-billed floor per table referenced per query, so splitting
         into N queries would multiply that floor by N for no benefit.
      4. Merge results into int_bigquery__column_cardinality.

    When use_query_text_attribution = false, the pre-filter is skipped and cardinality
    is calculated for all eligible columns per table (those present in
    int_bigquery__table_columns, which already excludes ineligible types and
    partitioning columns).

    Variables:
      clustering_key_cardinality_table_limit (default 10) — max candidate tables to scan
      clustering_candidates_lookback_days    (default 7)  — lookback window for usage pre-filter
      use_query_text_attribution             (default true) — toggles the heuristic pre-filter
  --#}

  {#--
    ref() calls below are evaluated unconditionally (outside `if execute`) so dbt's
    parser captures them as DAG edges on fct_bigquery__table_clustering_candidates.
    Wrapping them in `if execute` — as the body below otherwise would, since it also
    calls run_query() which needs a live connection — silently drops these edges,
    because dbt's ref-extraction pass runs with execute=False. Without these edges,
    a fresh-schema first run could try to build this model before the post-hook's
    dependencies (int_bigquery__column_query_stats, int_bigquery__table_columns,
    int_bigquery__column_cardinality) exist.

    col_stats_table is ref()'d only when use_query_text_attribution is on, since
    int_bigquery__column_query_stats is itself disabled (and ref()-ing a disabled
    model is a compile error) when that var is false.

    The candidates table is `this`, not ref('fct_bigquery__table_clustering_candidates')
    — this macro only ever runs as that model's own post-hook, so ref()-ing itself
    here would create a self-referencing cycle now that refs are captured eagerly.
  --#}
  {% set use_query_text_attribution = var('use_query_text_attribution', true) %}
  {% set col_cols_table    = ref('int_bigquery__table_columns') %}
  {% set cardinality_table = ref('int_bigquery__column_cardinality') %}
  {% if use_query_text_attribution %}
    {% set col_stats_table = ref('int_bigquery__column_query_stats') %}
  {% endif %}

  {% if execute and target.type == 'bigquery' %}

    {% set cardinality_limit = var('clustering_key_cardinality_table_limit', 10) %}
    {% set lookback_days = var('clustering_candidates_lookback_days', 7) %}

    {{ log("refresh_bigquery_column_cardinality: fetching top " ~ cardinality_limit ~ " candidates...", info=true) }}

    {% set candidates_sql %}
      select
          table_fqn,
          database_name,
          schema_name,
          table_name
      from {{ this }}
      where is_candidate = true
          and snapshot_date = (
              select max(snapshot_date) from {{ this }}
          )
      -- Spend the scan budget on tables that aren't already clustered first: an
      -- already-clustered table's column-cardinality data changes far less
      -- often (its shape is already keyed on whatever it's clustered by), so
      -- when candidates outnumber cardinality_limit, tables that have never
      -- been analyzed take priority over re-scanning one that's already
      -- optimized.
      qualify row_number() over (order by is_already_clustered asc, score desc) <= {{ cardinality_limit }}
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for row in candidates %}

        {% set table_fqn = row['table_fqn'] %}
        {% set db        = row['database_name'] %}
        {% set schema    = row['schema_name'] %}
        {% set table     = row['table_name'] %}

        {{ log("refresh_bigquery_column_cardinality: scanning cardinality for " ~ table_fqn, info=true) }}

        {% if use_query_text_attribution %}

          {% set columns_sql %}
            select distinct cqs.column_name
            from {{ col_stats_table }} as cqs
            inner join {{ col_cols_table }} as tc
                on cqs.table_fqn = tc.table_fqn
                and cqs.column_name = tc.column_name
            where cqs.table_fqn = '{{ table_fqn }}'
                and cqs.access_date >= date_sub(current_date(), interval {{ lookback_days }} day)
          {% endset %}

        {% else %}

          {% set columns_sql %}
            select distinct column_name
            from {{ col_cols_table }}
            where table_fqn = '{{ table_fqn }}'
          {% endset %}

        {% endif %}

        {% set columns_result = run_query(columns_sql) %}
        {% set columns_to_scan = columns_result.columns[0].values() if columns_result else [] %}

        {% if columns_to_scan | length > 0 %}

          {% set merge_sql %}
            merge into {{ cardinality_table }} as target
            using (
                with agg as (
                    select
                        count(*) as total_rows,
                        {% for col in columns_to_scan %}
                        approx_count_distinct({{ adapter.quote(col) }})
                            as {{ adapter.quote('distinct_' ~ loop.index0) }}
                        {%- if not loop.last %},{% endif %}
                        {% endfor %}
                    from `{{ db }}.{{ schema }}.{{ table }}`
                )
                select
                    '{{ table_fqn }}' as table_fqn,
                    unpivoted.column_name,
                    unpivoted.distinct_values,
                    agg.total_rows,
                    current_timestamp() as calculated_at
                from agg
                cross join unnest([
                    {% for col in columns_to_scan %}
                    struct(
                        '{{ col }}' as column_name,
                        {{ adapter.quote('distinct_' ~ loop.index0) }} as distinct_values
                    )
                    {%- if not loop.last %},{% endif %}
                    {% endfor %}
                ]) as unpivoted
            ) as source
            on target.table_fqn = source.table_fqn
                and target.column_name = source.column_name
            when matched then update set
                distinct_values = source.distinct_values,
                total_rows      = source.total_rows,
                calculated_at   = source.calculated_at
            when not matched then insert
                (table_fqn, column_name, distinct_values, total_rows, calculated_at)
            values
                (source.table_fqn, source.column_name, source.distinct_values, source.total_rows, source.calculated_at)
          {% endset %}

          {% do run_query(merge_sql) %}
          {{ log("refresh_bigquery_column_cardinality: merged cardinality for " ~ (columns_to_scan | length) ~ " column(s) on " ~ table_fqn, info=true) }}

        {% else %}
          {{ log("refresh_bigquery_column_cardinality: no eligible columns found for " ~ table_fqn ~ ", skipping.", info=true) }}
        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("refresh_bigquery_column_cardinality: no candidates found for today, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
