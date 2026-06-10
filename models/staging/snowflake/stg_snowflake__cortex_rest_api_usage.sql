{{ config(
    materialized='incremental',
    unique_key='request_id',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake' and var('ai_rest_api_enabled', true))
) }}

select
    request_id,
    start_time,
    end_time,
    model_name,
    tokens,
    tokens_granular,
    inference_region,
    user_id
from {{ source('snowflake_usage', 'cortex_rest_api_usage_history') }}
{% if is_incremental() %}
  where start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
