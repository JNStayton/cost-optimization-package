{{
  config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns',
    cluster_by=['to_date(query_start_time)'],
  )
}}

select
    query_id,
    start_time as query_start_time,
    query_hash,
    warehouse_name,
    warehouse_size,
    total_elapsed_time as total_elapsed_time_ms,
    bytes_scanned,
    query_load_percent,
    queued_overload_time as queued_overload_time_ms,
    queued_provisioning_time as queued_provisioning_time_ms,
    query_type,
    execution_time as execution_time_ms,
    partitions_scanned,
    partitions_total,
    bytes_spilled_to_local_storage as bytes_spilled_local,
    bytes_spilled_to_remote_storage as bytes_spilled_remote,
    query_text,
    session_id,
    execution_status,
    rows_inserted
from {{ ref('stg_snowflake__query_history') }}
{% if is_incremental() %}
where start_time >= (select dateadd(day, -1, max(query_start_time)) from {{ this }})
{% else %}
where start_time >= dateadd(day, -60, current_timestamp())
{% endif %}
