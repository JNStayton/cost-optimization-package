{{
  config(
    materialized='table',
  )
}}

{#--
  Agent-level optimization recommendations based on cost trends and usage patterns.

  Consumes int_snowflake__ai_agent_usage_daily to produce recommendations:
    - Runaway agent: credits growing rapidly (>50% WoW for 2+ weeks)
    - Low-usage high-cost: expensive agent with minimal adoption
    - Session bloat: individual requests are significantly more expensive than peers

  Grain: one row per (agent_name, agent_database_name, agent_schema_name)
--#}

{% set lookback_days = var('ai_lookback_days', 30) %}
{% set credit_rate_usd = var('ai_credit_rate_usd', 2) %}

with agent_30d as (
    select
        agent_name,
        agent_database_name,
        agent_schema_name,
        sum(total_credits) as total_credits_30d,
        sum(total_requests) as total_requests_30d,
        max(unique_users) as max_daily_users,
        count(distinct stats_date) as active_days,
        avg(avg_credits_per_request) as avg_credits_per_request,
        round(sum(total_credits) / nullif(count(distinct stats_date), 0), 4) as avg_daily_credits
    from {{ ref('int_snowflake__ai_agent_usage_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by agent_name, agent_database_name, agent_schema_name
),

agent_recent_7d as (
    select
        agent_name,
        agent_database_name,
        agent_schema_name,
        sum(total_credits) as total_credits_7d
    from {{ ref('int_snowflake__ai_agent_usage_daily') }}
    where stats_date >= dateadd(day, -7, current_date())
    group by agent_name, agent_database_name, agent_schema_name
),

agent_prior_7d as (
    select
        agent_name,
        agent_database_name,
        agent_schema_name,
        sum(total_credits) as total_credits_prior_7d
    from {{ ref('int_snowflake__ai_agent_usage_daily') }}
    where stats_date >= dateadd(day, -14, current_date())
      and stats_date < dateadd(day, -7, current_date())
    group by agent_name, agent_database_name, agent_schema_name
),

-- Median credits per request across all agents (for session bloat detection)
global_median as (
    select median(avg_credits_per_request) as median_credits_per_request
    from agent_30d
    where total_requests_30d > 10
),

scored as (
    select
        a30.*,
        coalesce(r7.total_credits_7d, 0) as total_credits_7d,
        coalesce(p7.total_credits_prior_7d, 0) as total_credits_prior_7d,
        case
            when coalesce(p7.total_credits_prior_7d, 0) > 0
                then (coalesce(r7.total_credits_7d, 0) - p7.total_credits_prior_7d) / p7.total_credits_prior_7d
            else null
        end as wow_growth_rate,
        gm.median_credits_per_request,
        round(a30.total_credits_30d * 12 * {{ credit_rate_usd }}, 2) as projected_annual_cost_usd,
        case
            -- Runaway: growing >50% WoW and meaningful spend
            when coalesce(p7.total_credits_prior_7d, 0) > 0
                 and (coalesce(r7.total_credits_7d, 0) - p7.total_credits_prior_7d) / p7.total_credits_prior_7d > 0.5
                 and a30.total_credits_30d > 5
                then 'runaway_cost'
            -- Low-usage high-cost: few users but expensive
            when a30.max_daily_users <= 3
                 and a30.total_credits_30d > 10
                then 'low_usage_high_cost'
            -- Session bloat: per-request cost is 2x+ the global median
            when gm.median_credits_per_request > 0
                 and a30.avg_credits_per_request > gm.median_credits_per_request * 2
                 and a30.total_credits_30d > 2
                then 'session_bloat'
            else 'healthy'
        end as recommendation_key
    from agent_30d as a30
    left join agent_recent_7d as r7
        on r7.agent_name = a30.agent_name
        and r7.agent_database_name = a30.agent_database_name
        and r7.agent_schema_name = a30.agent_schema_name
    left join agent_prior_7d as p7
        on p7.agent_name = a30.agent_name
        and p7.agent_database_name = a30.agent_database_name
        and p7.agent_schema_name = a30.agent_schema_name
    cross join global_median as gm
)

select
    current_date() as snapshot_date,
    current_timestamp() as analyzed_at,
    agent_name,
    agent_database_name,
    agent_schema_name,
    agent_database_name || '.' || agent_schema_name || '.' || agent_name as agent_fqn,
    total_credits_30d,
    total_requests_30d,
    max_daily_users,
    active_days,
    avg_credits_per_request,
    avg_daily_credits,
    wow_growth_rate,
    projected_annual_cost_usd,
    case
        when recommendation_key = 'runaway_cost'
            then 'Agent cost growing rapidly — review usage'
        when recommendation_key = 'low_usage_high_cost'
            then 'Low adoption, high cost — consolidate or deprecate'
        when recommendation_key = 'session_bloat'
            then 'High per-request cost — optimize agent tools/prompts'
        else 'Healthy — no action needed'
    end as recommendation,
    case
        when recommendation_key = 'runaway_cost'
            then 'Agent ' || agent_name || ' credits grew '
                || round(coalesce(wow_growth_rate, 0) * 100, 0)
                || '% WoW (' || round(total_credits_7d, 2)
                || ' credits this week vs ' || round(total_credits_prior_7d, 2)
                || ' prior). Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'low_usage_high_cost'
            then 'Agent ' || agent_name || ' has only ' || max_daily_users
                || ' user(s) but consumes ' || round(total_credits_30d, 2)
                || ' credits/month. Consider whether this agent is delivering value.'
        when recommendation_key = 'session_bloat'
            then 'Agent ' || agent_name || ' averages '
                || round(avg_credits_per_request, 4)
                || ' credits/request (global median: '
                || round(median_credits_per_request, 4)
                || '). Review tool calls and prompt complexity.'
        else 'Agent ' || agent_name || ' is operating within normal parameters.'
    end as recommendation_reason
from scored
order by
    case recommendation_key
        when 'runaway_cost' then 1
        when 'low_usage_high_cost' then 2
        when 'session_bloat' then 3
        else 4
    end,
    projected_annual_cost_usd desc
