{{ config(
    materialized='incremental',
    unique_key=['warehouse_name', 'start_time'],
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
) }}

select
    service_type,
    warehouse_name,
    start_time,
    end_time,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services
from {{ source('snowflake_usage', 'metering_history') }}
where service_type in (
    'AI_SERVICES',
    'CORTEX_AGENTS',
    'CORTEX_CODE_CLI',
    'CORTEX_CODE_SNOWSIGHT',
    'SNOWFLAKE_INTELLIGENCE'
)
{% if is_incremental() %}
  and start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  and start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
