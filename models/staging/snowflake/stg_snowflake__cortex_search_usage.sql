{{ config(
    materialized='incremental',
    unique_key=['start_time', 'service_name'],
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
) }}

select
    start_time,
    end_time,
    credits,
    database_name,
    schema_name,
    service_name
from {{ source('snowflake_usage', 'cortex_search_serving_usage_history') }}
{% if is_incremental() %}
  where start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
