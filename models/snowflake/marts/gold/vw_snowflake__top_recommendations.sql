{{
  config(
    materialized='view',
  )
}}

{#--
  Top recommendations across all domains, ranked by estimated annual savings.
  Deduplicated by node_id (same logical model across environments shows once).
  Only actionable items (no Monitor/Stable).

  This is the flagship gold-layer view — the executive action list.
--#}

with env_counts as (
    select node_id, count(distinct table_fqn) as environment_count
    from {{ ref('int_snowflake__dbt_relation_history') }}
    where node_id is not null
    group by node_id
),

ranked as (
    select
        ar.*,
        coalesce(ec.environment_count, 1) as environment_count,
        row_number() over (
            partition by ar.dedup_key, ar.domain
            order by ar.env_priority asc, ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as env_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
    where ar.backlog_status = 'actionable'
)

select
    dense_rank() over (order by estimated_annual_savings_usd desc nulls last, score desc) as priority_rank,
    domain,
    effort_category,
    node_id,
    coalesce(node_model_name, model_name) as model_name,
    node_project_name as project_name,
    table_fqn,
    warehouse_name,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    score,
    actionable_sql,
    environment_count,
    target_name,
    snapshot_date
from ranked
where env_rank = 1
order by priority_rank
