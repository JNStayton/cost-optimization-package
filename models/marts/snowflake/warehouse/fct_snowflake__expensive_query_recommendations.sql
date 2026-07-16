{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Surfaces the most expensive dbt-authored queries by projected annual cost,
  grouped by query_hash so repeated runs of the same logical query are
  aggregated together.

  Recommendation tiers (same as find_expensive_dbt_queries macro):
    High Cost (warn) — projected annual cost > high_cost_threshold_usd
    Tracked   (info) — above the min_total_credits floor but below the high bar

  Annual cost is projected from the lookback-window credit consumption
  multiplied by credit_rate_usd. Tune credit_rate_usd to your contract rate.

  When use_query_attribution = false (Standard edition), credits are estimated
  from warehouse_metering_history prorated by elapsed time share. The
  credits_from_attribution column flags which method was used.

  Trend direction compares the 7-day projected annual cost to the 30-day
  projected annual cost:
    Worsening  — 7-day projection > 30-day projection by more than 20%
    Improving  — 7-day projection < 30-day projection by more than 20%
    Stable     — within 20% of the 30-day baseline

  Controlled by the following dbt variables:
    - expensive_query_lookback_days       (default 30)
    - expensive_query_credit_rate_usd     (default 2)
    - expensive_query_high_cost_threshold (default 10000)
    - expensive_query_min_total_credits   (default 0.1)
    - expensive_query_top_n               (default 50)
--#}

{% set lookback_days        = var('expensive_query_lookback_days', 30) %}
{% set credit_rate_usd      = var('expensive_query_credit_rate_usd', 2) %}
{% set high_cost_threshold  = var('expensive_query_high_cost_threshold', 10000) %}
{% set min_total_credits    = var('expensive_query_min_total_credits', 0.1) %}
{% set top_n                = var('expensive_query_top_n', 50) %}

with window_30d as (
    select
        query_hash,
        any_value(dbt_node_id)                                      as dbt_node_id,
        any_value(warehouse_name)                                   as warehouse_name,
        any_value(warehouse_size)                                   as warehouse_size,
        any_value(credits_from_attribution)                         as credits_from_attribution,
        sum(total_runs)                                             as total_runs_30d,
        round(avg(avg_elapsed_sec), 2)                              as avg_elapsed_sec,
        round(sum(total_credits), 6)                                as total_credits_30d,
        round(sum(total_credits) / {{ lookback_days }}, 6)          as avg_daily_credits,
        round((sum(total_credits) / {{ lookback_days }}) * 365, 2)  as estimated_annual_credits,
        round(
            (sum(total_credits) / {{ lookback_days }}) * 365 * {{ credit_rate_usd }},
            2
        )                                                           as estimated_annual_cost_usd
    from {{ ref('int_snowflake__warehouse_expensive_queries_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by query_hash
    having sum(total_credits) >= {{ min_total_credits }}
),

window_7d as (
    select
        query_hash,
        round(sum(total_credits) / 7, 6)                            as avg_daily_credits_7d,
        round((sum(total_credits) / 7) * 365 * {{ credit_rate_usd }}, 2)
                                                                    as estimated_annual_cost_usd_7d
    from {{ ref('int_snowflake__warehouse_expensive_queries_daily') }}
    where stats_date >= dateadd(day, -7, current_date())
    group by query_hash
),

scored as (
    select
        w30.query_hash,
        w30.dbt_node_id,
        w30.warehouse_name,
        w30.warehouse_size,
        w30.credits_from_attribution,
        w30.total_runs_30d,
        w30.avg_elapsed_sec,
        w30.total_credits_30d,
        w30.avg_daily_credits,
        w30.estimated_annual_credits,
        w30.estimated_annual_cost_usd,
        coalesce(w7.estimated_annual_cost_usd_7d, w30.estimated_annual_cost_usd)
                                                                    as estimated_annual_cost_usd_7d,
        case
            when coalesce(w7.estimated_annual_cost_usd_7d, w30.estimated_annual_cost_usd)
                 > w30.estimated_annual_cost_usd * 1.2
                then 'Worsening'
            when coalesce(w7.estimated_annual_cost_usd_7d, w30.estimated_annual_cost_usd)
                 < w30.estimated_annual_cost_usd * 0.8
                then 'Improving'
            else 'Stable'
        end                                                         as cost_trend,
        case
            when w30.estimated_annual_cost_usd > {{ high_cost_threshold }}
                then 'high_cost'
            else 'tracked'
        end                                                         as recommendation_key
    from window_30d as w30
    left join window_7d as w7 on w7.query_hash = w30.query_hash
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    {{ credit_rate_usd }}                                           as credit_rate_usd,
    query_hash,
    dbt_node_id,
    warehouse_name,
    warehouse_size,
    credits_from_attribution,
    total_runs_30d,
    avg_elapsed_sec,
    total_credits_30d,
    avg_daily_credits,
    estimated_annual_credits,
    estimated_annual_cost_usd,
    estimated_annual_cost_usd_7d,
    cost_trend,
    case
        when recommendation_key = 'high_cost'
            then 'Review for refactor opportunities (high projected cost)'
        else
            'Monitor — recurring credit consumption'
    end                                                             as recommendation,
    case
        when recommendation_key = 'high_cost'
            then 'Projected $' || estimated_annual_cost_usd || '/yr ('
                || estimated_annual_credits || ' credits/yr) exceeds the $'
                || {{ high_cost_threshold }} || ' threshold. '
                || 'Ran ' || total_runs_30d || ' time(s) over the last '
                || {{ lookback_days }} || ' days at avg ' || avg_elapsed_sec || 's per run. '
                || 'Cost trend: ' || cost_trend || '. '
                || case
                    when not credits_from_attribution
                        then 'Note: credit estimate is approximate (Standard edition — '
                            || 'set use_query_attribution = true for precise attribution).'
                    else ''
                   end
        else
            'Projected $' || estimated_annual_cost_usd || '/yr ('
            || estimated_annual_credits || ' credits/yr). '
            || 'Ran ' || total_runs_30d || ' time(s) over the last '
            || {{ lookback_days }} || ' days at avg ' || avg_elapsed_sec || 's per run. '
            || 'Cost trend: ' || cost_trend || '. '
            || case
                when not credits_from_attribution
                    then 'Note: credit estimate is approximate (Standard edition).'
                else ''
               end
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'high_cost' then 1
        else 2
    end,
    estimated_annual_cost_usd desc
limit {{ top_n }}
