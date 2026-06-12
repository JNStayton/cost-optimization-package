{{ config(
    materialized='incremental',
    unique_key='session_id',
    on_schema_change='append_new_columns',
) }}

select
    session_id,
    created_on,
    parse_json(client_environment):APPLICATION::string as application_name,
    parse_json(client_environment):OS::string          as client_os,
    parse_json(client_environment):VERSION::string     as client_version,
    client_environment
from {{ source('snowflake_usage', 'sessions') }}
where parse_json(client_environment):APPLICATION::string = 'dbt'
{% if is_incremental() %}
  and created_on >= (select dateadd(day, -1, max(created_on)) from {{ this }})
{% else %}
  and created_on >= dateadd(day, -30, current_timestamp())
{% endif %}
