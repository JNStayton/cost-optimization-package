{{
  config(
    materialized='view',
  )
}}

{#--
  Full optimization backlog for sprint planning and agent consumption.
  Includes ALL tiers (actionable + monitor + stable) with full context.
  Grouped by effort_category (quick wins first), then by estimated savings.

  Each row is self-contained — an agent can create a ticket from any single row.
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
            partition by ar.dedup_key, ar.domain, ar.recommendation
            order by ar.env_priority asc, ar.score desc
        ) as env_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
)

select
    domain,
    effort_category,
    backlog_status,
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
    dbt_config_template,
    validate_uniqueness_sql,
    environment_count,
    target_name,
    snapshot_date
from ranked
where env_rank = 1
order by
    case effort_category
        when 'config_change' then 1
        when 'sql_refactor' then 2
        when 'architecture' then 3
        else 4
    end,
    case backlog_status
        when 'actionable' then 1
        when 'monitor' then 2
        else 3
    end,
    estimated_annual_savings_usd desc nulls last,
    score desc
