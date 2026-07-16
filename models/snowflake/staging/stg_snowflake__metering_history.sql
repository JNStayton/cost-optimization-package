{{ config(
    materialized='incremental',
    unique_key=['service_type', 'entity_name', 'start_time'],
    on_schema_change='append_new_columns',
) }}

{#--
  Staging model for METERING_HISTORY. Filters to AI/Cortex service types.

  Column mapping:
    - METERING_HISTORY.NAME -> entity_name (the service/function name, not a warehouse)
    - METERING_HISTORY.DATABASE_NAME -> database_name (available for some service types)
    - METERING_HISTORY.SCHEMA_NAME -> schema_name (available for some service types)

  Note: warehouse_name does NOT exist in METERING_HISTORY.
  The NAME column contains the entity name which varies by service_type.
--#}

select
    service_type,
    name as entity_name,
    database_name,
    schema_name,
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
