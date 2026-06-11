{{ config(
    materialized='incremental',
    unique_key='request_id',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
) }}

{#--
  Staging model for CORTEX_AGENT_USAGE_HISTORY.

  Column mapping (source -> alias):
    - AGENT_DATABASE_NAME -> agent_database_name
    - AGENT_SCHEMA_NAME -> agent_schema_name
    - USER_NAME -> user_name (in addition to user_id)

  Note: The source view prefixes database/schema with "AGENT_".
  These are NOT bare "database_name"/"schema_name".
--#}

select
    request_id,
    parent_request_id,
    start_time,
    end_time,
    agent_name,
    agent_id,
    agent_database_name,
    agent_schema_name,
    user_id,
    user_name,
    token_credits,
    tokens,
    role_names
from {{ source('snowflake_usage', 'cortex_agent_usage_history') }}
{% if is_incremental() %}
  where start_time >= (select dateadd(day, -1, max(start_time)) from {{ this }})
{% else %}
  where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
