{{
  config(
    materialized='view',
  )
}}

{#--
  dbt model-level optimizations: materialization, clustering, and incremental config.
  These are actions taken IN dbt code (model configs, SQL changes).
  Excludes warehouse-level and AI-level recommendations, except spillage_route_models
  (a snowflake_warehouse config on the model).

  Shows ALL signals per model with priority_tier for ordering.
  Enriched with clustering key detail from int_snowflake__clustering_key_summary.

  Priority tiers:
    P1 = actionable now (high savings or spillage co-occurrence)
    P2 = root cause fix (standard model-level optimization)
--#}

with env_counts as (
    select
        node_id,
        count(distinct table_fqn) as deployed_relation_count,
        array_agg(distinct dbt_cloud_environment_id) as environment_ids
    from {{ ref('int_snowflake__dbt_relation_history') }}
    where node_id is not null
      and not coalesce(is_excluded, false)
    group by node_id
),

ranked as (
    select
        ar.*,
        -- Null on rows that aren't about a model (e.g. warehouse settings)
        iff(ar.node_id is null, null, coalesce(ec.deployed_relation_count, 1)) as deployed_relation_count,
        ec.environment_ids,
        ck.suggested_clustering_key,
        ck.additional_clustering_candidates,
        -- Incremental confidence context (from fact model)
        icr.confidence_score as incremental_confidence_score,
        icr.recommendation_status as incremental_recommendation_status,
        icr.assumptions as incremental_assumptions,
        icr.blocking_signals as incremental_blocking_signals,
        -- View chains (materialize_as_table only): which view in the chain is recommended,
        -- and the recompute cost behind its savings
        tm.chain_role,
        tm.chosen_view_for_chain,
        tm.recompute_cost_s,
        tm.recompute_cost_source,
        row_number() over (
            partition by ar.dedup_key, ar.domain
            order by ar.priority_tier,
                ar.estimated_annual_savings_usd desc nulls last,
                ar.score desc
        ) as env_rank
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    left join env_counts as ec on ec.node_id = ar.node_id
    -- Clustering keys only on clustering rows: each row shows the fields of its own fix
    left join {{ ref('int_snowflake__clustering_key_summary') }} as ck
        on ck.table_fqn = ar.table_fqn
        and ar.domain = 'clustering'
    left join {{ ref('fct_snowflake__incremental_config_recommendations') }} as icr
        on icr.table_fqn = ar.table_fqn
        and (ar.signal_id like 'apply_incremental%' or ar.signal_id = 'convert_to_incremental')
    left join {{ ref('fct_snowflake__table_materialization_candidates') }} as tm
        on tm.table_fqn = ar.table_fqn
        and ar.signal_id = 'materialize_as_table'
    where (ar.domain in ('materialization', 'clustering')
           -- Job-level spillage routing is a dbt config change on the model
           or ar.signal_id = 'spillage_route_models')
      and ar.backlog_status = 'actionable'
)

select
    node_id,
    node_project_name as project_name,
    coalesce(node_model_name, model_name) as model_name,
    domain,
    signal_id,
    priority_tier,
    effort_category,
    table_fqn,
    recommendation,
    recommendation_reason,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    snowflake_ddl,
    suggested_clustering_key,
    additional_clustering_candidates,
    dbt_model_config,
    identified_unique_key,
    -- Incremental confidence (null for clustering/materialization-table recs)
    incremental_confidence_score,
    incremental_recommendation_status,
    incremental_assumptions,
    incremental_blocking_signals,
    chain_role,
    chosen_view_for_chain,
    recompute_cost_s,
    recompute_cost_source,
    deployed_relation_count,
    environment_ids,
    target_name,
    snapshot_date
from ranked
where env_rank = 1
    -- Suppress clustering recommendations with no actionable key
    and not (domain = 'clustering' and suggested_clustering_key is null)
order by coalesce(node_model_name, model_name), priority_tier, estimated_annual_savings_usd desc nulls last
