{{
  config(
    materialized='view',
  )
}}

{#--
  KPI card view — one row per domain showing total opportunity.
  Designed for dashboard top-level summary tiles.
--#}

with actionable as (
    select *
    from {{ ref('int_snowflake__all_recommendations') }}
    where backlog_status = 'actionable'
)

select
    domain,
    count(*) as total_recommendations,
    count(case when effort_category = 'config_change' then 1 end) as quick_win_count,
    round(sum(coalesce(estimated_annual_cost_usd, 0)), 2) as estimated_annual_cost_usd,
    round(sum(coalesce(estimated_annual_savings_usd, 0)), 2) as estimated_annual_savings_usd,
    round(
        sum(coalesce(estimated_annual_savings_usd, 0))
        / nullif(sum(coalesce(estimated_annual_cost_usd, 0)), 0) * 100,
        1
    ) as savings_pct,
    -- Top recommendation in this domain
    max_by(recommendation, coalesce(estimated_annual_savings_usd, 0)) as top_recommendation,
    max(coalesce(estimated_annual_savings_usd, 0)) as top_recommendation_savings_usd
from actionable
group by domain
order by estimated_annual_savings_usd desc
