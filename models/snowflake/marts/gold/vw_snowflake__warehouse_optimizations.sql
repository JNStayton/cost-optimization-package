{{
  config(
    materialized='view',
  )
}}

{#--
  Warehouse-level optimizations: ALL signals per warehouse including config
  recommendations, spillage, and expensive queries. Ordered by priority_tier.

  Audience: Snowflake admins.
  Grain: one row per (warehouse, signal). A warehouse may appear multiple times.

  Priority tiers:
    P1 = actionable now (safe config changes)
    P2 = root cause fix or promoted conditional config
    P3 = deferred config (waiting on model-level fixes)
    P4 = monitor
--#}

with warehouse_config as (
    select warehouse_name, current_size, warehouse_category
    from {{ ref('int_snowflake__warehouse_config') }}
)

select
    ar.warehouse_name,
    wc.current_size as warehouse_current_size,
    wc.warehouse_category,
    ar.signal_id,
    ar.priority_tier,
    ar.effort_category,
    ar.backlog_status,
    ar.recommendation,
    ar.recommendation_reason,
    ar.estimated_annual_cost_usd,
    ar.estimated_annual_savings_usd,
    ar.score,
    ar.snowflake_ddl,
    -- Model context (when signal is tied to a specific model, e.g., spillage)
    ar.node_id,
    coalesce(ar.node_model_name, ar.model_name) as model_name,
    ar.table_fqn,
    ar.node_project_name as project_name,
    ar.snapshot_date
from {{ ref('int_snowflake__all_recommendations') }} as ar
left join warehouse_config as wc on wc.warehouse_name = ar.warehouse_name
where ar.domain = 'warehouse'
  and ar.backlog_status in ('actionable', 'monitor')
order by ar.warehouse_name, ar.priority_tier, ar.estimated_annual_savings_usd desc nulls last
