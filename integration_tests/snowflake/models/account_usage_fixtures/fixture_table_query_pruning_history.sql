{{ config(alias='table_query_pruning_history') }}
{#- Stand-in for ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY. Types come from the real view.
    As in Snowflake, partitions scanned and pruned are TOTALS across NUM_QUERIES queries.
    DEMO_EVENTS has 100 micropartitions and three query shapes, 20 queries in all:
      - 15 queries that scan it once:        1,350 scanned + 150 pruned (100 per query)
      -  3 queries on a smaller, older copy:    162 scanned +  18 pruned  (60 per query)
      -  2 queries that scan it ten times:    1,800 scanned + 200 pruned (1,000 per query)
    Scan ratio 3,312 / 3,680 = 90% (poor pruning). The query-weighted median of partitions
    per query is 100, the table's size; the plain average (184) and minimum (60) are not. -#}
select interval_start_time, interval_end_time, warehouse_id, warehouse_name, table_id, table_name, schema_id, schema_name,
       database_id, database_name, query_hash, query_parameterized_hash, num_queries, partitions_scanned, partitions_pruned,
       rows_scanned, rows_pruned, rows_matched, aggregate_query_elapsed_time, aggregate_query_compilation_time, aggregate_query_execution_time
from snowflake.account_usage.table_query_pruning_history where false
{% for shape, queries, scanned, pruned in [('single_scan', 15, 1350, 150), ('older_copy', 3, 162, 18), ('ten_scans', 2, 1800, 200)] %}
union all
select dateadd(hour, -2, current_timestamp()), dateadd(hour, -1, current_timestamp()), 1, 'FIXTURE_WH', 900001, 'DEMO_EVENTS', 1,
       upper('{{ target.schema }}'), 1, upper('{{ target.database }}'), 'hash_events_{{ shape }}', 'phash_events_{{ shape }}',
       {{ queries }}, {{ scanned }}, {{ pruned }}, {{ scanned * 2000 }}, {{ pruned * 2000 }}, {{ queries * 2000 }},
       {{ queries * 5000 }}, {{ queries * 50 }}, {{ queries * 4950 }}
{%- endfor %}
