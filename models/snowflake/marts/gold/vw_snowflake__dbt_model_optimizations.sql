{{
  config(
    materialized='view',
  )
}}

{#--
  dbt model-level optimizations: materialization, clustering, and incremental config.
  These are actions taken IN dbt code (model configs, SQL changes).
  Excludes warehouse-level and AI-level recommendations.

  Enriched with clustering key detail from fct_snowflake__clustering_key_candidates.
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

clustering_keys as (
    select
        table_fqn,
        listagg(column_name, ', ') within group (order by recommended_key_position) as suggested_clustering_key
    from (
        select distinct table_fqn, column_name, recommended_key_position
        from {{ ref('fct_snowflake__clustering_key_candidates') }}
        where snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__clustering_key_candidates') }})
    )
    group by table_fqn
),

ranked as (
    select
        ar.*,
        coalesce(ec.environment_count, 1) as environment_count,
        ec.environment_ids,
        ck.suggested_clustering_key,
        row_number() over (
            partition by ar.dedup_key, ar.domain
            order by ar.estimated_annual_savings_usd desc nulls last,
                case ar.effort_category when 'config_change' then 1 when 'sql_refactor' then 2 else 3 end,
                ar.score desc
        ) as env_rank,
        row_number() over (
            partition by coalesce(ar.node_id, ar.entity_name)
            order by ar.estimated_annual_savings_usd desc nulls last, ar.score desc
        ) as priority
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
    left join clustering_keys as ck on ck.table_fqn = ar.table_fqn
    where ar.domain in ('materialization', 'clustering')
      and ar.backlog_status = 'actionable'
)

select
    node_id,
    node_project_name as project_name,
    coalesce(node_model_name, model_name) as model_name,
    domain,
    effort_category,
    priority,
    table_fqn,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    score,
    snowflake_ddl,
    suggested_clustering_key,
    case
        when domain = 'clustering' and suggested_clustering_key is not null
            then '{% raw %}{{ config(cluster_by=[{% endraw %}''' || replace(suggested_clustering_key, ', ', ''', ''') || '''{% raw %}]) }}{% endraw %}'
        else dbt_model_config
    end as dbt_model_config,
    identified_unique_key,
    environment_count,
    environment_ids,
    target_name,
    snapshot_date
from ranked
where env_rank = 1
order by coalesce(node_model_name, model_name), priority
