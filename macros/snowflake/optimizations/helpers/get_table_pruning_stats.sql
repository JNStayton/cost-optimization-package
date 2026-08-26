{% macro get_table_pruning_stats(database_name, schema_name, table_name, lookback_days=7) %}
  {#--
    Returns per-table pruning stats from TABLE_QUERY_PRUNING_HISTORY.
    Standalone helper — queries Snowflake directly without model dependencies.

    Returns columns: TOTAL_PARTITIONS_SCANNED, TOTAL_PARTITIONS_PRUNED,
                     TOTAL_QUERY_COUNT, AVG_EXECUTION_TIME_MS
  --#}

  {% set sql %}
    select
        coalesce(sum(partitions_scanned), 0) as total_partitions_scanned,
        coalesce(sum(partitions_pruned), 0) as total_partitions_pruned,
        coalesce(sum(num_queries), 0) as total_query_count,
        case
            when sum(num_queries) > 0
            then sum(aggregate_query_execution_time) / sum(num_queries)
            else 0
        end as avg_execution_time_ms
    from snowflake.account_usage.table_query_pruning_history
    where database_name = '{{ database_name }}'
      and schema_name = '{{ schema_name }}'
      and table_name = '{{ table_name }}'
      and interval_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
  {% endset %}

  {% set results = run_query(sql) %}
  {{ return(results) }}

{% endmacro %}
