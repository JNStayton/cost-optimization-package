{% macro refresh_column_cardinality() %}

  {#--
    Populates int_snowflake__column_cardinality with real APPROX_COUNT_DISTINCT
    values for the top N clustering candidates identified by
    fct_snowflake__table_clustering_candidates.

    Runs automatically as a post-hook on fct_snowflake__table_clustering_candidates,
    so it fires after candidates are built and before fct_snowflake__clustering_key_candidates
    reads from the cardinality table.

    Steps:
      1. Query top N candidates from the already-built candidates fact model.
      2. For each candidate, look up which columns have actual query access history
         in int_snowflake__column_query_stats (pre-filter — avoids scanning
         every column of every large table).
      3. Run APPROX_COUNT_DISTINCT only against pre-filtered columns.
      4. Merge results into int_snowflake__column_cardinality.

    When use_access_history_attribution = false (Standard edition), the pre-filter
    step is skipped and cardinality is calculated for all eligible columns per table
    (those present in int_snowflake__table_columns, which already excludes unsupported
    data types like VARIANT/ARRAY/OBJECT).

    Variables:
      clustering_key_cardinality_table_limit (default 10) — max candidate tables to scan
      clustering_candidates_lookback_days    (default 7)  — lookback window for usage pre-filter
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set cardinality_limit = var('clustering_key_cardinality_table_limit', 10) %}
    {% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
    {% set use_access_history = var('use_access_history_attribution', true) %}

    {# Resolve table references once #}
    {% set candidates_table = ref('fct_snowflake__table_clustering_candidates') %}
    {% set col_stats_table = ref('int_snowflake__column_query_stats') %}
    {% set col_cols_table = ref('int_snowflake__table_columns') %}
    {% set cardinality_table = ref('int_snowflake__column_cardinality') %}

    {{ log("refresh_column_cardinality: fetching top " ~ cardinality_limit ~ " candidates...", info=true) }}

    {% set candidates_sql %}
      select
          table_fqn,
          database_name,
          schema_name,
          table_name
      from {{ candidates_table }}
      where is_candidate = true
          and snapshot_date = current_date()
      qualify row_number() over (order by score desc) <= {{ cardinality_limit }}
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for row in candidates %}

        {% set table_fqn   = row['TABLE_FQN'] %}
        {% set db          = row['DATABASE_NAME'] %}
        {% set schema      = row['SCHEMA_NAME'] %}
        {% set table       = row['TABLE_NAME'] %}

        {{ log("refresh_column_cardinality: scanning cardinality for " ~ table_fqn, info=true) }}

        {# Step 2: get columns to scan #}
        {% if use_access_history %}

          {# Enterprise+: only columns with actual query access in the lookback window #}
          {% set columns_sql %}
            select distinct cqs.column_name
            from {{ col_stats_table }} as cqs
            inner join {{ col_cols_table }} as tc
                on cqs.table_fqn = tc.table_fqn
                and cqs.column_name = tc.column_name
            where cqs.table_fqn = '{{ table_fqn }}'
                and cqs.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
                and tc.data_type not in ('VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY')
          {% endset %}

        {% else %}

          {# Standard: all columns for this table, excluding types unsupported as clustering keys #}
          {% set columns_sql %}
            select distinct column_name
            from {{ col_cols_table }}
            where table_fqn = '{{ table_fqn }}'
                and data_type not in ('VARIANT', 'ARRAY', 'OBJECT', 'GEOGRAPHY', 'GEOMETRY')
          {% endset %}

        {% endif %}

        {% set columns_result = run_query(columns_sql) %}
        {% set columns_to_scan = columns_result.columns[0].values() if columns_result else [] %}

        {% if columns_to_scan | length > 0 %}

          {# Step 3: run APPROX_COUNT_DISTINCT and merge results #}
          {% set merge_sql %}
            merge into {{ cardinality_table }} as target
            using (
              select
                  '{{ table_fqn }}' as table_fqn,
                  column_name,
                  distinct_values,
                  total_rows,
                  current_timestamp() as calculated_at
              from (
                  {% for col in columns_to_scan %}
                    select
                        '{{ col }}' as column_name,
                        approx_count_distinct({{ adapter.quote(col) }}) as distinct_values,
                        count(*) as total_rows
                    from {{ db }}.{{ schema }}.{{ table }}
                    {% if not loop.last %}union all{% endif %}
                  {% endfor %}
              )
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
          {{ log("refresh_column_cardinality: merged cardinality for " ~ (columns_to_scan | length) ~ " column(s) on " ~ table_fqn, info=true) }}

        {% else %}
          {{ log("refresh_column_cardinality: no eligible columns found for " ~ table_fqn ~ ", skipping.", info=true) }}
        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("refresh_column_cardinality: no candidates found for today, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
