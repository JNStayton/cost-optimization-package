{{ config(
    materialized='incremental',
    unique_key='query_id',
    enabled=(target.type == 'snowflake')
) }}

select
    query_id, 
    start_time, 
    query_hash, 
    warehouse_name, 
    warehouse_size, 
    total_elapsed_time, 
    bytes_scanned, 
    query_load_percent, 
    queued_overload_time, 
    query_type, 
    execution_time, 
    partitions_scanned, 
    partitions_total,
    bytes_spilled_to_local_storage, 
    bytes_spilled_to_remote_storage, 
    query_text, 
    session_id, 
    execution_status
from {{ source('snowflake_usage', 'query_history') }}

{% if is_incremental() %}
  where start_time >= (select dateadd(day, -7, max(start_time)) from {{ this }})
{% endif %}