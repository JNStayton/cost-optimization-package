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
  Filtered to dbt_monitored_projects (excludes installed package models by default).
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
    where (
        ar.node_project_name in (
            {% for proj in monitored_projects %}
              '{{ proj }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        or ar.node_id is null
    )
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
    environment_ids,
    dbt_cloud_environment_id,
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
