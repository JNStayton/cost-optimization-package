{{
  config(
    materialized='view',
  )
}}

{#--
  Model-centric view that collapses recommendations by logical dbt model (node_id).
  Same model in multiple environments shows once — highest-impact env wins.
  Includes cost estimation fields and environment metadata.

  Grain: one row per (node_id, domain, recommendation)
--#}

{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

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
            partition by ar.dedup_key, ar.domain, ar.recommendation
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as env_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
    where ar.backlog_status = 'actionable'
      and (
          ar.node_project_name in (
              {% for proj in monitored_projects %}
                '{{ proj }}'{% if not loop.last %}, {% endif %}
              {% endfor %}
          )
          or ar.node_id is null
      )
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
    r.dbt_cloud_environment_id as primary_environment_id,
    r.environment_count,
    r.environment_ids,
    af.all_table_fqns,
    r.recommendation,
    r.recommendation_reason,
    r.score as max_score,
    r.estimated_annual_cost_usd,
    r.estimated_annual_savings_usd,
    r.actionable_sql,
    r.snapshot_date
from ranked as r
left join all_fqns as af
    on af.dedup_key = r.dedup_key
    and af.domain = r.domain
    and af.recommendation = r.recommendation
where r.env_rank = 1
order by r.estimated_annual_savings_usd desc nulls last, r.score desc nulls last
