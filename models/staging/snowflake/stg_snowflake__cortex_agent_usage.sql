{{ config(
    materialized='incremental',
    unique_key='request_id',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
) }}

select
    request_id,
    parent_request_id,
    start_time,
    end_time,
    agent_name,
    database_name,
    schema_name,
    user_id,
    token_credits,
    role_names
from {{ source('snowflake_usage', 'cortex_agent_usage_history') }}
{% if is_incremental() %}
  where start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
