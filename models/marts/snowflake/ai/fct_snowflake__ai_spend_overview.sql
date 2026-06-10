{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  AI spend overview with trend analysis across all Cortex service types.
  Provides executive-level summary of AI credit consumption, growth rates,
  and projected annual costs.

  Grain: one row per service_type (aggregated over lookback window).

  Controlled by:
    - ai_spend_lookback_days (default 30)
    - ai_credit_rate_usd    (default 2)
--#}

{% set lookback_days   = var('ai_spend_lookback_days', 30) %}
{% set credit_rate_usd = var('ai_credit_rate_usd', 2) %}

with daily_spend as (
    select
        stats_date,
        service_type,
        total_credits
    from {{ ref('int_snowflake__ai_spend_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
),

service_summary as (
    select
        service_type,
        sum(total_credits)                                          as total_credits,
        count(distinct stats_date)                                  as active_days,
        round(sum(total_credits) / nullif(count(distinct stats_date), 0), 4)
                                                                    as avg_daily_credits,
        round(
            (sum(total_credits) / nullif(count(distinct stats_date), 0)) * 365,
            2
        )                                                           as projected_annual_credits,
        round(
            (sum(total_credits) / nullif(count(distinct stats_date), 0)) * 365 * {{ credit_rate_usd }},
            2
        )                                                           as projected_annual_cost_usd
    from daily_spend
    group by service_type
),

recent_7d as (
    select
        service_type,
        sum(total_credits)                                          as credits_7d
    from daily_spend
    where stats_date >= dateadd(day, -7, current_date())
    group by service_type
),

prior_7d as (
    select
        service_type,
        sum(total_credits)                                          as credits_prior_7d
    from daily_spend
    where stats_date >= dateadd(day, -14, current_date())
      and stats_date < dateadd(day, -7, current_date())
    group by service_type
),

total_account as (
    select sum(total_credits) as account_total_credits
    from service_summary
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    {{ credit_rate_usd }}                                           as credit_rate_usd,
    ss.service_type,
    ss.total_credits,
    ss.active_days,
    ss.avg_daily_credits,
    ss.projected_annual_credits,
    ss.projected_annual_cost_usd,
    round(ss.total_credits / nullif(ta.account_total_credits, 0) * 100, 1)
                                                                    as pct_of_total_ai_spend,
    coalesce(r7.credits_7d, 0)                                     as credits_last_7d,
    coalesce(p7.credits_prior_7d, 0)                               as credits_prior_7d,
    case
        when coalesce(p7.credits_prior_7d, 0) = 0 and coalesce(r7.credits_7d, 0) > 0
            then 'New'
        when coalesce(r7.credits_7d, 0) > coalesce(p7.credits_prior_7d, 0) * 1.2
            then 'Growing'
        when coalesce(r7.credits_7d, 0) < coalesce(p7.credits_prior_7d, 0) * 0.8
            then 'Declining'
        else 'Stable'
    end                                                             as wow_trend
from service_summary as ss
cross join total_account as ta
left join recent_7d as r7 on r7.service_type = ss.service_type
left join prior_7d as p7 on p7.service_type = ss.service_type
order by ss.total_credits desc
