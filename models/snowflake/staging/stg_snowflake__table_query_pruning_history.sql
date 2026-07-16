{{ config(
    materialized='incremental',
    unique_key='pruning_history_key',
    on_schema_change='append_new_columns',
    cluster_by=['to_date(start_time)']
) }}

select
    md5(
        coalesce(to_varchar(interval_start_time), '') || '|' ||
        coalesce(to_varchar(table_id), '') || '|' ||
        coalesce(to_varchar(warehouse_id), '') || '|' ||
        coalesce(query_hash, '')
    ) as pruning_history_key,
    interval_start_time,
    interval_end_time,
    table_id,
    table_name,
    schema_id,
    schema_name,
    database_id,
    database_name,
    warehouse_id,
    warehouse_name,
    query_hash,
    query_parameterized_hash,
    num_queries,
    aggregate_query_elapsed_time as query_elapsed_time_ms,
    aggregate_query_compilation_time as query_compilation_time_ms,
    aggregate_query_execution_time as query_execution_time_ms,
    partitions_scanned,
    partitions_pruned,
    rows_scanned,
    rows_pruned,
    rows_matched
from {{ source('snowflake_usage', 'table_query_pruning_history') }}
{% if is_incremental() %}
where interval_start_time >= (select dateadd(day, -7, max(interval_start_time)) from {{ this }})
{% endif %}
