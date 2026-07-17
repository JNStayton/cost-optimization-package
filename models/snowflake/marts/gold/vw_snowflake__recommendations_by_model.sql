{{
  config(
    materialized='view',
  )
}}

{#--
  Gold-layer view that collapses recommendations by logical dbt model (node_id).

  Joins physical-grain fact models (via int_snowflake__all_recommendations) to
  int_snowflake__dbt_relation_history to map each table_fqn to its logical node_id.
  Then deduplicates so the same dbt model appearing in multiple environments
  (dev, staging, prod) shows as ONE recommendation rather than 3 duplicates.

  Priority order for picking the "representative" environment:
    1. prod (target_name contains 'prod')
    2. staging (target_name contains 'stag')
    3. Everything else (dev, default, CI, etc.)

  If the same model has DIFFERENT recommendations across environments
  (e.g., "Materialize as TABLE" in dev, "Monitor" in prod), both appear —
  they are genuinely different signals worth surfacing.

  Grain: one row per (node_id, domain, recommendation)
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
    where ar.backlog_status = 'actionable'
),

all_fqns as (
    select
        dedup_key,
        domain,
        recommendation,
        array_agg(distinct table_fqn) as all_table_fqns
    from {{ ref('int_snowflake__all_recommendations') }}
    where table_fqn is not null
    group by 1, 2, 3
)

select
    r.node_id,
    r.node_project_name as project_name,
    coalesce(r.node_model_name, r.model_name) as model_name,
    r.domain,
    r.effort_category,
    r.table_fqn as primary_table_fqn,
    r.target_name as primary_target_name,
    r.environment_count,
    af.all_table_fqns,
    r.recommendation,
    r.recommendation_reason,
    r.score as max_score,
    r.estimated_annual_cost_usd,
    r.estimated_annual_savings_usd,
    r.actionable_sql,
    r.snapshot_date,
    case when r.env_priority = 1 then true else false end as has_prod_relation,
    case when r.env_priority > 1 and r.environment_count = 1 then true else false end as has_nonprod_only
from ranked as r
left join all_fqns as af
    on af.dedup_key = r.dedup_key
    and af.domain = r.domain
    and af.recommendation = r.recommendation
where r.env_rank = 1
order by r.estimated_annual_savings_usd desc nulls last, r.score desc nulls last
