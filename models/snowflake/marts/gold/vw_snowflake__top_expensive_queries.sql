{{
  config(
    materialized='view',
  )
}}

{#--
  Top expensive recurring queries within the project scope.
  Shows the highest-cost queries running on warehouses used by this project,
  including both dbt model builds and downstream consumer queries (dashboards, ad-hoc).

  Audience: dbt developers + Snowflake admins.
  Grain: one row per query_hash (deduplicated recurring query pattern).
  Ordered by estimated annual cost descending, limited to top 10.
--#}

select
    ar.warehouse_name,
    ar.node_id,
    coalesce(ar.node_model_name, ar.model_name) as model_name,
    ar.table_fqn,
    ar.entity_name as query_hash,
    ar.estimated_annual_cost_usd,
    ar.estimated_annual_savings_usd,
    ar.score as total_credits_30d,
    ar.recommendation,
    ar.recommendation_reason,
    ar.backlog_status,
    ar.node_project_name as project_name,
    ar.snapshot_date
from {{ ref('int_snowflake__all_recommendations') }} as ar
where ar.domain = 'warehouse'
  and ar.effort_category = 'sql_refactor'
  and ar.backlog_status in ('actionable', 'monitor')
order by ar.estimated_annual_cost_usd desc nulls last
limit 10
