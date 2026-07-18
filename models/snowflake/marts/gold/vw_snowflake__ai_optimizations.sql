{{
  config(
    materialized='view',
  )
}}

{#--
  AI/Cortex optimization recommendations.
  Covers: model cost (downgrade, prompt bloat, batching), token efficiency
  (failure rates, I/O ratios, caching), agent cost trends, and spend overview.
  Audience: AI/ML teams and platform engineers.
--#}

with ranked as (
    select
        ar.*,
        row_number() over (
            partition by ar.entity_name, ar.recommendation
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as dedup_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.domain = 'ai'
      and ar.backlog_status in ('actionable', 'monitor')
)

select
    entity_name as service_or_model,
    effort_category,
    backlog_status,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    score,
    actionable_sql,
    snapshot_date
from ranked
where dedup_rank = 1
order by estimated_annual_savings_usd desc nulls last, score desc
