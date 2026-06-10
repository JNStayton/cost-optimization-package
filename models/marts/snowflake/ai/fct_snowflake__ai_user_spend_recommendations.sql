{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Per-user AI spend recommendations: identifies top consumers, detects spikes,
  flags governance gaps, and suggests budget thresholds.

  Grain: one row per user.

  Controlled by:
    - ai_spend_lookback_days            (default 30)
    - ai_credit_rate_usd                (default 2)
    - ai_user_spike_threshold_pct       (default 200)
    - ai_user_concentration_threshold_pct (default 50)
    - ai_min_credits_for_recommendation (default 1)
--#}

{% set lookback_days            = var('ai_spend_lookback_days', 30) %}
{% set credit_rate_usd          = var('ai_credit_rate_usd', 2) %}
{% set spike_threshold_pct      = var('ai_user_spike_threshold_pct', 200) %}
{% set concentration_threshold  = var('ai_user_concentration_threshold_pct', 50) %}
{% set min_credits              = var('ai_min_credits_for_recommendation', 1) %}

with user_stats_30d as (
    select
        uu.user_id,
        sum(uu.total_credits)                                       as total_credits_30d,
        sum(uu.query_count)                                         as total_queries_30d,
        count(distinct uu.stats_date)                               as active_days,
        round(sum(uu.total_credits) / nullif(count(distinct uu.stats_date), 0), 4)
                                                                    as avg_daily_credits,
        sum(uu.untagged_query_count)                                as untagged_queries,
        sum(uu.tagged_query_count)                                  as tagged_queries
    from {{ ref('int_snowflake__ai_user_usage_daily') }} as uu
    where uu.stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by uu.user_id
    having sum(uu.total_credits) >= {{ min_credits }}
),

user_stats_7d as (
    select
        user_id,
        sum(total_credits)                                          as total_credits_7d,
        count(distinct stats_date)                                  as active_days_7d
    from {{ ref('int_snowflake__ai_user_usage_daily') }}
    where stats_date >= dateadd(day, -7, current_date())
    group by user_id
),

account_total as (
    select sum(total_credits_30d) as total_account_credits
    from user_stats_30d
),

users as (
    select user_id, name as user_name, email, default_role
    from {{ source('snowflake_usage', 'users') }}
),

scored as (
    select
        u30.user_id,
        usr.user_name,
        usr.email,
        usr.default_role,
        u30.total_credits_30d,
        u30.total_queries_30d,
        u30.active_days,
        u30.avg_daily_credits,
        u30.untagged_queries,
        u30.tagged_queries,
        coalesce(u7.total_credits_7d, 0)                            as total_credits_7d,
        round(
            coalesce(u7.total_credits_7d, 0) / nullif(u7.active_days_7d, 0),
            4
        )                                                           as avg_daily_credits_7d,
        round(u30.total_credits_30d / nullif(at.total_account_credits, 0) * 100, 1)
                                                                    as pct_of_account_spend,
        case
            when u30.avg_daily_credits > 0
                 and coalesce(u7.total_credits_7d, 0) / nullif(u7.active_days_7d, 0)
                     > u30.avg_daily_credits * ({{ spike_threshold_pct }} / 100.0)
                then 'spike'
            when u30.total_credits_30d / nullif(at.total_account_credits, 0) * 100
                 > {{ concentration_threshold }}
                then 'concentration'
            when u30.untagged_queries > u30.tagged_queries
                then 'no_attribution'
            else 'healthy'
        end                                                         as recommendation_key
    from user_stats_30d as u30
    cross join account_total as at
    left join user_stats_7d as u7 on u7.user_id = u30.user_id
    left join users as usr on usr.user_id = u30.user_id
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    {{ credit_rate_usd }}                                           as credit_rate_usd,
    user_id,
    user_name,
    email,
    default_role,
    total_credits_30d,
    total_queries_30d,
    active_days,
    avg_daily_credits,
    total_credits_7d,
    avg_daily_credits_7d,
    pct_of_account_spend,
    untagged_queries,
    tagged_queries,
    round(avg_daily_credits * 365 * {{ credit_rate_usd }}, 2)      as projected_annual_cost_usd,
    case
        when recommendation_key = 'spike'
            then 'Investigate usage spike (recent spend well above baseline)'
        when recommendation_key = 'concentration'
            then 'High concentration — consider budget cap or workload distribution'
        when recommendation_key = 'no_attribution'
            then 'Governance gap — most queries lack QUERY_TAG attribution'
        else 'Healthy — no action needed'
    end                                                             as recommendation,
    case
        when recommendation_key = 'spike'
            then 'Recent 7-day average (' || round(avg_daily_credits_7d, 2) || ' credits/day) is '
                || round(avg_daily_credits_7d / nullif(avg_daily_credits, 0) * 100, 0)
                || '% of the ' || {{ lookback_days }} || '-day baseline ('
                || round(avg_daily_credits, 2) || ' credits/day). '
                || 'Investigate for runaway queries or unexpected usage patterns.'
        when recommendation_key = 'concentration'
            then user_name || ' accounts for ' || pct_of_account_spend
                || '% of total AI spend (' || round(total_credits_30d, 2) || ' credits). '
                || 'Consider per-user budget caps or distributing AI workloads across roles.'
        when recommendation_key = 'no_attribution'
            then untagged_queries || ' of ' || (tagged_queries + untagged_queries)
                || ' queries lack a QUERY_TAG. '
                || 'Set session-level QUERY_TAG for cost attribution by project/team.'
        else user_name || ' — ' || round(total_credits_30d, 2) || ' credits over '
            || active_days || ' days. Usage is stable and within normal range.'
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'spike'          then 1
        when 'concentration'  then 2
        when 'no_attribution' then 3
        else 4
    end,
    total_credits_30d desc
