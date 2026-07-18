{{
  config(
    materialized='view',
  )
}}

{#--
  User-level cost attribution across expensive queries and AI usage.
  Aggregates credits consumed by user for chargeback and awareness dashboards.
  Audience: Engineering managers, finance, platform teams.
--#}

{% set credit_rate_usd = var('credit_rate_usd', 2) %}

with query_users as (
    select
        top_user_name as user_name,
        top_role_name as role_name,
        sum(total_credits_30d) as query_credits_30d,
        count(*) as expensive_query_count,
        max_by(query_hash, total_credits_30d) as top_expensive_query_hash
    from {{ ref('fct_snowflake__expensive_query_recommendations') }}
    where top_user_name is not null
    group by top_user_name, top_role_name
),

ai_users as (
    select
        user_name,
        default_role as role_name,
        total_credits_30d as ai_credits_30d,
        total_queries_30d as ai_query_count
    from {{ ref('fct_snowflake__ai_user_spend_recommendations') }}
    where user_name is not null
)

select
    coalesce(qu.user_name, au.user_name) as user_name,
    coalesce(qu.role_name, au.role_name) as role_name,
    coalesce(qu.query_credits_30d, 0) as query_credits_30d,
    coalesce(au.ai_credits_30d, 0) as ai_credits_30d,
    coalesce(qu.query_credits_30d, 0) + coalesce(au.ai_credits_30d, 0) as combined_credits_30d,
    round((coalesce(qu.query_credits_30d, 0) + coalesce(au.ai_credits_30d, 0)) * 12 * {{ credit_rate_usd }}, 2) as estimated_annual_cost_usd,
    qu.top_expensive_query_hash,
    coalesce(qu.expensive_query_count, 0) as expensive_query_count,
    coalesce(au.ai_query_count, 0) as ai_query_count,
    case
        when coalesce(qu.query_credits_30d, 0) + coalesce(au.ai_credits_30d, 0) > 10
            then 'High cost user — review workload patterns'
        when coalesce(qu.query_credits_30d, 0) + coalesce(au.ai_credits_30d, 0) > 2
            then 'Moderate cost user — monitor trends'
        else 'Low cost user'
    end as recommendation
from query_users as qu
full outer join ai_users as au
    on au.user_name = qu.user_name
order by combined_credits_30d desc
