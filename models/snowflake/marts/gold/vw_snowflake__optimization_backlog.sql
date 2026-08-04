{{
  config(
    materialized='view',
  )
}}

{#--
  Optimization backlog for sprint planning and agent consumption.
  Shows P1 (actionable now) and P2 (root cause fix) signals.
  Each row is self-contained — an agent can create a ticket from any single row.

  Ordered by priority_tier first (P1 before P2), then by savings.
--#}

with env_counts as (
    select
        node_id,
        count(distinct table_fqn) as environment_count,
        array_agg(distinct dbt_cloud_environment_id) as environment_ids
    from {{ ref('int_snowflake__dbt_relation_history') }}
    where node_id is not null
    group by node_id
),

ranked as (
    select
        ar.*,
        coalesce(ec.environment_count, 1) as environment_count,
        ec.environment_ids,
        row_number() over (
            partition by ar.dedup_key, ar.domain, ar.signal_id
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as env_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
    where ar.priority_tier in (1, 2)
      and ar.backlog_status = 'actionable'
)

select
    domain,
    signal_id,
    priority_tier,
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
    snowflake_ddl,
    dbt_model_config,
    identified_unique_key,
    case
        when snowflake_ddl is not null then 'snowflake_ddl'
        when dbt_model_config is not null then 'dbt_config'
        else 'investigation'
    end as action_type,
    environment_count,
    environment_ids,
    target_name,
    snapshot_date
from ranked
where env_rank = 1
order by priority_tier, estimated_annual_savings_usd desc nulls last, score desc
