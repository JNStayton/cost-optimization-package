{{
  config(
    materialized='view',
  )
}}

{#--
  Gold-layer view that collapses recommendations by logical dbt model (node_id).

  Joins physical-grain fact models to int_snowflake__dbt_relation_history to map
  each table_fqn to its logical node_id. Then deduplicates so the same dbt model
  appearing in multiple environments (dev, staging, prod) shows as ONE recommendation
  rather than 3 duplicates.

  Priority order for picking the "representative" environment:
    1. prod (target_name contains 'prod')
    2. staging (target_name contains 'stag')
    3. Everything else (dev, default, CI, etc.)

  If the same model has DIFFERENT recommendations across environments
  (e.g., "Materialize as TABLE" in dev, "Monitor" in prod), both appear —
  they are genuinely different signals worth surfacing.

  Grain: one row per (node_id, domain, recommendation)
--#}

{% set priority_order = var('environment_priority_order', ['prod', 'staging', 'dev']) %}

with all_recommendations as (
    -- Table materialization candidates
    select
        table_fqn,
        'materialization' as domain,
        recommendation,
        recommendation_reason,
        materialization_score as score,
        null::float as credits,
        select_count as query_count,
        snapshot_date
    from {{ ref('fct_snowflake__table_materialization_candidates_v2') }}
    where recommendation != 'Monitor'

    union all

    -- Incremental materialization candidates
    select
        table_fqn,
        'materialization' as domain,
        recommendation,
        recommendation_reason,
        compute_waste_score as score,
        null::float as credits,
        table_build_count as query_count,
        snapshot_date
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }}
    where recommendation != 'Monitor'

    union all

    -- Table clustering candidates
    select
        table_fqn,
        'clustering' as domain,
        recommendation_tier || ' — clustering candidate' as recommendation,
        recommendation_reason,
        score,
        null::float as credits,
        select_count as query_count,
        snapshot_date
    from {{ ref('fct_snowflake__table_clustering_candidates') }}
    where is_candidate = true

    union all

    -- Warehouse spillage
    select
        table_fqn,
        'warehouse' as domain,
        recommendation,
        recommendation_reason,
        total_gb_spilled_local + total_gb_spilled_remote as score,
        null::float as credits,
        total_runs as query_count,
        snapshot_date
    from {{ ref('fct_snowflake__warehouse_spillage_recommendations') }}
    where recommendation not like 'Not available%'
),

enriched as (
    select
        ar.*,
        rh.node_id,
        rh.project_name,
        rh.model_name,
        rh.target_name,
        -- Priority: prod-like=1, staging-like=2, else=3
        -- Recognizes common production target names: prod, default, production, main
        case
            when lower(rh.target_name) in ('prod', 'default', 'production', 'main')
                or lower(rh.target_name) like '%prod%' then 1
            when lower(rh.target_name) like '%stag%' then 2
            else 3
        end as env_priority
    from all_recommendations as ar
    left join {{ ref('int_snowflake__dbt_relation_history') }} as rh
        on rh.table_fqn = ar.table_fqn
),

-- Count environments per node_id
env_counts as (
    select node_id, count(distinct table_fqn) as environment_count
    from {{ ref('int_snowflake__dbt_relation_history') }}
    where node_id is not null
    group by node_id
),

-- Rank within each (node_id, domain, recommendation) to pick representative env
ranked as (
    select
        e.*,
        coalesce(ec.environment_count, 1) as environment_count,
        row_number() over (
            partition by coalesce(e.node_id, e.table_fqn), e.domain, e.recommendation
            order by e.env_priority asc, e.score desc
        ) as env_rank
    from enriched as e
    left join env_counts as ec on ec.node_id = e.node_id
),

-- Collect all FQNs for each logical group
all_fqns as (
    select
        coalesce(node_id, table_fqn) as group_key,
        domain,
        recommendation,
        array_agg(distinct table_fqn) as all_table_fqns
    from enriched
    group by 1, 2, 3
)

select
    r.node_id,
    r.project_name,
    r.model_name,
    r.domain,
    r.table_fqn as primary_table_fqn,
    r.target_name as primary_target_name,
    r.environment_count,
    af.all_table_fqns,
    r.recommendation,
    r.recommendation_reason,
    r.score as max_score,
    r.credits as total_credits,
    r.query_count,
    r.snapshot_date,
    case when r.env_priority = 1 then true else false end as has_prod_relation,
    case when r.env_priority > 1 and r.environment_count = 1 then true else false end as has_nonprod_only
from ranked as r
left join all_fqns as af
    on af.group_key = coalesce(r.node_id, r.table_fqn)
    and af.domain = r.domain
    and af.recommendation = r.recommendation
where r.env_rank = 1
order by r.score desc nulls last
