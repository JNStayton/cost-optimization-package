{{
  config(
    materialized='view'
  )
}}

{#--
  Gold rollup — one consumer-facing surface for every Databricks cost
  recommendation.

  Unions each recommendation mart into a common shape (identity, domain,
  recommendation label, domain-relative score) and collapses the same logical
  dbt node across environments into a single representative row.

  Notes:
    - model_run_summary is intentionally excluded — it is a performance-trend /
      monitoring mart, not a recommendation surface.
    - liquid_clustering has no native recommendation label, so one is synthesized.
    - `score` is DOMAIN-RELATIVE — it ranks rows within a (node, domain,
      recommendation) group for environment selection and display. It is NOT
      comparable across domains (GB saved vs a file-fragmentation score, etc.).
    - Cross-environment dedup keys on the dbt node id (unique_id). Environment is
      inferred from the physical schema/catalog name (there is no explicit env
      column in the graph). This only collapses rows when the marts actually
      contain the same node built in more than one environment; in a single
      target run it is typically a no-op, but it is correct and future-proof.

  Env priority is driven by var('environment_priority_order', ['prod','staging','dev']).
--#}

with recommendations as (

    -- Incremental materialization candidates -------------------------------
    select
        'incremental_model_candidates'                 as source_mart,
        'incremental'                                  as domain,
        snapshot_date,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model                                      as node_id,
        model_name,
        suggested_incremental_strategy                 as recommendation,
        cast(score as double)                          as score,
        downstream_model_count                         as downstream_count
    from {{ ref('fct_databricks__incremental_model_candidates') }}
    where is_candidate = true
      and snapshot_date = (select max(snapshot_date) from {{ ref('fct_databricks__incremental_model_candidates') }})

    union all

    -- Table materialization candidates (view/ephemeral -> table) -----------
    select
        'table_materialization_candidates'             as source_mart,
        'materialization'                              as domain,
        snapshot_date,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model                                      as node_id,
        model_name,
        recommendation,
        cast(coalesce(composite_chain_score, materialization_score, 0) as double) as score,
        downstream_table_count                         as downstream_count
    from {{ ref('fct_databricks__table_materialization_candidates') }}
    where recommendation = 'Materialize as TABLE'

    union all

    -- Snapshot optimization candidates -------------------------------------
    select
        'snapshot_optimization_candidates'             as source_mart,
        'snapshot'                                     as domain,
        snapshot_date,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_snapshot                                   as node_id,
        snapshot_name                                  as model_name,
        recommendation,
        cast(estimated_monthly_savings_gb as double)   as score,
        downstream_model_count                         as downstream_count
    from {{ ref('fct_databricks__snapshot_optimization_candidates') }}
    where recommendation not in ('Healthy', 'No runs in lookback window')
      and snapshot_date = (select max(snapshot_date) from {{ ref('fct_databricks__snapshot_optimization_candidates') }})

    union all

    -- OPTIMIZE / predictive-optimization candidates ------------------------
    select
        'optimize_candidates'                          as source_mart,
        'storage'                                      as domain,
        snapshot_date,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model                                      as node_id,
        cast(null as string)                           as model_name,
        recommended_action                             as recommendation,
        cast(score as double)                          as score,
        cast(null as bigint)                           as downstream_count
    from {{ ref('fct_databricks__optimize_candidates') }}
    where is_candidate = true
      and snapshot_date = (select max(snapshot_date) from {{ ref('fct_databricks__optimize_candidates') }})

    union all

    -- Liquid clustering candidates (synthesized label) ---------------------
    select
        'liquid_clustering_candidates'                 as source_mart,
        'clustering'                                   as domain,
        snapshot_date,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model                                      as node_id,
        cast(null as string)                           as model_name,
        'Enable liquid clustering'                     as recommendation,
        cast(score as double)                          as score,
        cast(null as bigint)                           as downstream_count
    from {{ ref('fct_databricks__liquid_clustering_candidates') }}
    where is_candidate = true
      and snapshot_date = (select max(snapshot_date) from {{ ref('fct_databricks__liquid_clustering_candidates') }})

),

-- Infer an environment rank from the physical schema/catalog name so the same
-- logical node built in multiple environments collapses to one representative row.
env_ranked as (
    select
        r.*,
        coalesce(node_id, table_fqn) as dedup_key,
        case
            when lower(schema_name)   rlike '(^|_)(prod|production|main|default)($|_)'
              or lower(database_name) rlike '(^|_)(prod|production|main|default)($|_)' then 1
            when lower(schema_name)   rlike '(^|_)(staging|stage|stg|preprod|uat)($|_)'
              or lower(database_name) rlike '(^|_)(staging|stage|stg|preprod|uat)($|_)' then 2
            else 3
        end as env_priority
    from recommendations as r
),

-- Per-group aggregates (grouped, not windowed — avoids collect_set-over-window
-- which is not portable across Spark/Databricks runtime versions).
grouped as (
    select
        dedup_key,
        domain,
        recommendation,
        count(*)                                             as environment_count,
        collect_set(table_fqn)                               as all_table_fqns,
        (max(case when env_priority = 1 then 1 else 0 end) = 1) as has_prod_relation
    from env_ranked
    group by dedup_key, domain, recommendation
),

ranked as (
    select
        e.*,
        row_number() over (
            partition by e.dedup_key, e.domain, e.recommendation
            order by e.env_priority asc, e.score desc nulls last, e.table_fqn
        ) as env_rank
    from env_ranked as e
)

select
    '{{ target.type }}'      as platform,
    r.snapshot_date,
    r.source_mart,
    r.domain,
    r.node_id,
    r.model_name,
    r.database_name,
    r.schema_name,
    r.table_name,
    r.table_fqn              as primary_table_fqn,
    g.all_table_fqns,
    g.environment_count,
    g.has_prod_relation,
    r.recommendation,
    r.score,
    r.downstream_count
from ranked as r
inner join grouped as g
    on  g.dedup_key      = r.dedup_key
    and g.domain         = r.domain
    and g.recommendation = r.recommendation
where r.env_rank = 1
order by
    case when g.has_prod_relation then 0 else 1 end,
    coalesce(r.downstream_count, 0) desc,
    r.score desc nulls last
