{{ config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
) }}

select
    start_time,
    end_time,
    function_name,
    model_name,
    query_id,
    warehouse_id,
    role_names,
    query_tag,
    user_id,
    metrics,
    credits,
    is_completed
from {{ source('snowflake_usage', 'cortex_ai_functions_usage_history') }}
{% if is_incremental() %}
  where start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
