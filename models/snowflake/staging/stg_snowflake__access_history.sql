{{ config(
    materialized='incremental',
    unique_key='query_id',
    cluster_by=['to_date(query_start_time)']
) }}

select 
    query_id, 
    query_start_time, 
    objects_modified, 
    base_objects_accessed,
    direct_objects_accessed
from {{ source('snowflake_usage', 'access_history') }}
{% if is_incremental() %}
  where query_start_time >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(query_start_time)) from {{ this }})
{% else %}
  where query_start_time >= dateadd(day, -14, current_timestamp())
{% endif %}