{{
  config(
    materialized='view',
  )
}}

{#--
  Environment drill-down view. Does NOT collapse by node_id — every physical
  FQN gets its own row. Filterable by dbt_cloud_environment_id.

  Use cases:
    WHERE dbt_cloud_environment_id = '12345'  → single env recommendations
    Compare same model across envs            → which env has worse performance?
--#}

{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

with env_counts as (
    select
        node_id,
        count(distinct table_fqn) as environment_count
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
    ar.dbt_cloud_environment_id,
    coalesce(ec.environment_count, 1) as environment_count,
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
  and (
      ar.node_project_name in (
          {% for proj in monitored_projects %}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
          {% endfor %}
      )
      or ar.node_id is null
  )
order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
