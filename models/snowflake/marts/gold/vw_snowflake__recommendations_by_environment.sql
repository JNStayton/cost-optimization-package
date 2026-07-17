{{
  config(
    materialized='view',
  )
}}

{#--
  Environment drill-down view. Does NOT collapse by node_id — every physical
  FQN gets its own row. Filterable by target_name/environment.

  Use cases:
    WHERE target_name LIKE '%prod%'       → prod-only recommendations
    WHERE has_nonprod_only = true         → models not yet in prod
    Compare same model across envs        → which env is worse?
--#}

with env_counts as (
    select node_id, count(distinct table_fqn) as environment_count
    from {{ ref('int_snowflake__dbt_relation_history') }}
    where node_id is not null
    group by node_id
)

select
    ar.domain,
    ar.effort_category,
    ar.backlog_status,
    ar.node_id,
    coalesce(ar.node_model_name, ar.model_name) as model_name,
    ar.node_project_name as project_name,
    ar.table_fqn,
    ar.warehouse_name,
    ar.target_name,
    coalesce(ec.environment_count, 1) as environment_count,
    case when ar.env_priority = 1 then true else false end as has_prod_relation,
    case when ar.env_priority > 1 and coalesce(ec.environment_count, 1) = 1 then true else false end as has_nonprod_only,
    ar.recommendation,
    ar.recommendation_reason,
    ar.estimated_annual_cost_usd,
    ar.estimated_annual_savings_usd,
    ar.score,
    ar.actionable_sql,
    ar.snapshot_date
from {{ ref('int_snowflake__all_recommendations') }} as ar
left join env_counts as ec on ec.node_id = ar.node_id
where ar.backlog_status = 'actionable'
order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
