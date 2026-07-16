{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ai_agent_usage_daily_key',
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily Cortex Agent credit consumption per agent.
  Grain: one row per (agent_name, agent_database_name, agent_schema_name, stats_date).
--#}

select
    md5(
        coalesce(to_varchar(cast(start_time as date)), '') || '|' ||
        coalesce(agent_name, '') || '|' ||
        coalesce(agent_database_name, '') || '|' ||
        coalesce(agent_schema_name, '')
    )                                                   as ai_agent_usage_daily_key,
    cast(start_time as date)                            as stats_date,
    agent_name,
    agent_database_name,
    agent_schema_name,
    count(distinct request_id)                          as total_requests,
    count(distinct user_id)                             as unique_users,
    sum(token_credits)                                  as total_credits,
    round(avg(token_credits), 6)                        as avg_credits_per_request,
    count(distinct parent_request_id)                   as distinct_parent_requests
from {{ ref('stg_snowflake__cortex_agent_usage') }}
{% if is_incremental() %}
where cast(start_time as date) >= (
    select dateadd(day, -1, coalesce(max(stats_date), '1970-01-01'::date))
    from {{ this }}
)
{% endif %}
group by cast(start_time as date), agent_name, agent_database_name, agent_schema_name
