{{ config(
    materialized='incremental',
    unique_key='session_id',
    cluster_by=['to_date(created_on)'],
    on_schema_change='append_new_columns',
) }}

-- try_parse_json, not parse_json: a client_environment that isn't valid JSON is
-- treated as a non-dbt session (excluded) instead of failing the model.
select
    session_id,
    created_on,
    try_parse_json(client_environment):APPLICATION::string as application_name,
    try_parse_json(client_environment):OS::string          as client_os,
    try_parse_json(client_environment):VERSION::string     as client_version,
    client_environment
from {{ source('snowflake_usage', 'sessions') }}
where try_parse_json(client_environment):APPLICATION::string = 'dbt'
{% if is_incremental() %}
  and created_on >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(created_on)) from {{ this }})
{% else %}
  and created_on >= dateadd(day, -30, current_timestamp())
{% endif %}
