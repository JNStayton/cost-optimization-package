{{ config(alias='table_query_pruning_history') }}
{#- Stand-in for ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY. Types come from the real view.
    DEMO_EVENTS scans 90% of its partitions (90 scanned, 10 pruned): poor pruning. -#}
select interval_start_time, interval_end_time, warehouse_id, warehouse_name, table_id, table_name, schema_id, schema_name,
       database_id, database_name, query_hash, query_parameterized_hash, num_queries, partitions_scanned, partitions_pruned,
       rows_scanned, rows_pruned, rows_matched, aggregate_query_elapsed_time, aggregate_query_compilation_time, aggregate_query_execution_time
from snowflake.account_usage.table_query_pruning_history where false
union all
select dateadd(hour, -2, current_timestamp()), dateadd(hour, -1, current_timestamp()), 1, 'FIXTURE_WH', 900001, 'DEMO_EVENTS', 1,
       upper('{{ target.schema }}'), 1, upper('{{ target.database }}'), 'hash_demo_events', 'phash_demo_events', 20, 90, 10,
       180000, 20000, 40000, 100000, 1000, 99000
