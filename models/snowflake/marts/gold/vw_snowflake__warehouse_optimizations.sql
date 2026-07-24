{{
  config(
    materialized='view',
  )
}}

{#--
  Warehouse-level optimizations: sizing, spillage, expensive queries, provisioning.
  These are actions taken in Snowflake (ALTER WAREHOUSE, auto-suspend config).
  Audience: Snowflake admins.

  Includes warehouse_category from int_snowflake__warehouse_config for context.
--#}

with warehouse_config as (
    select warehouse_name, current_size, warehouse_category, is_smallest_size
    from {{ ref('int_snowflake__warehouse_config') }}
),

ranked as (
    select
        ar.*,
        wc.current_size as warehouse_current_size,
        wc.warehouse_category,
        row_number() over (
            partition by ar.entity_name, ar.recommendation
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as dedup_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join warehouse_config as wc on wc.warehouse_name = ar.warehouse_name
    where ar.domain = 'warehouse'
      and ar.backlog_status in ('actionable', 'monitor')
      and {{ scope_filter('ar.node_project_name', 'ar.node_id') }}
)

select
    warehouse_name,
    warehouse_current_size,
    warehouse_category,
    -- Symptom taxonomy (replaces generic effort_category)
    case
        when recommendation like '%Scale down%' or recommendation like '%auto-suspend%'
            then 'idle_credit_consumption'
        when recommendation like '%Scale up%'
            then 'queued_provisioning'
        when recommendation like '%multi-cluster%'
            then 'query_overload'
        when recommendation like '%Gen2%'
            then 'compute_inefficiency'
        when recommendation_reason like '%spill%'
            then 'spillage_overflow'
        when recommendation like '%Monitor%' and recommendation_reason like '%Worsening%'
            then 'query_cost_growth'
        else 'general_inefficiency'
    end as symptom,
    backlog_status,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    score,
    snowflake_ddl,
    -- Model context (when warehouse rec is tied to a specific model)
    node_id,
    coalesce(node_model_name, model_name) as model_name,
    node_project_name as project_name,
    table_fqn,
    snapshot_date
from ranked
where dedup_rank = 1
order by estimated_annual_savings_usd desc nulls last, score desc
