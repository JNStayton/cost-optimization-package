{{
  config(
    materialized='view',
  )
}}

{#--
  Cross-domain signal detection. For each model with 2+ optimization signals
  from different domains, surfaces the signals and picks the primary
  recommendation using the priority hierarchy.

  Reads from int_all_recommendations (single source of truth) rather than
  querying fact models directly. Only counts actionable signals.

  One row per model (node_id) — naturally deduped.
--#}

with signals_per_model as (
    select
        ar.node_id,
        coalesce(ar.node_model_name, ar.model_name) as model_name,
        ar.table_fqn,
        ar.node_project_name as project_name,
        max(ar.warehouse_name) as warehouse_name,
        -- Collect distinct signal categories (not individual signal_ids)
        array_agg(distinct
            case
                when ar.signal_id like 'spillage%' then 'spillage'
                when ar.signal_id like 'add_clustering%' then 'clustering'
                when ar.signal_id like 'materialize%' then 'materialization'
                when ar.signal_id like 'convert_to_incremental%' or ar.signal_id like 'apply_incremental%' then 'incremental'
                when ar.signal_id like 'expensive_query%' then 'expensive_query'
                else ar.domain
            end
        ) as signals,
        count(distinct
            case
                when ar.signal_id like 'spillage%' then 'spillage'
                when ar.signal_id like 'add_clustering%' then 'clustering'
                when ar.signal_id like 'materialize%' then 'materialization'
                when ar.signal_id like 'convert_to_incremental%' or ar.signal_id like 'apply_incremental%' then 'incremental'
                when ar.signal_id like 'expensive_query%' then 'expensive_query'
                else ar.domain
            end
        ) as signal_count,
        -- Top priority signal for this model
        min(ar.priority_tier) as top_priority,
        min_by(ar.signal_id, ar.priority_tier) as top_signal_id,
        min_by(ar.recommendation, ar.priority_tier) as top_recommendation,
        min_by(ar.domain, ar.priority_tier) as top_domain,
        -- Presence flags for root cause analysis
        max(iff(ar.signal_id like 'spillage%', 1, 0)) = 1 as has_spillage,
        max(iff(ar.signal_id like 'add_clustering%', 1, 0)) = 1 as has_clustering,
        max(iff(ar.signal_id like 'materialize%', 1, 0)) = 1 as has_materialization,
        max(iff(ar.signal_id like 'convert_to_incremental%' or ar.signal_id like 'apply_incremental%', 1, 0)) = 1 as has_incremental,
        max(iff(ar.signal_id like 'expensive_query%', 1, 0)) = 1 as has_expensive_query,
        max(ar.snapshot_date) as snapshot_date
    from {{ ref('int_snowflake__all_recommendations') }} as ar
    where ar.backlog_status = 'actionable'
      and ar.node_id is not null
    group by ar.node_id, coalesce(ar.node_model_name, ar.model_name), ar.table_fqn, ar.node_project_name
    having count(distinct
        case
            when ar.signal_id like 'spillage%' then 'spillage'
            when ar.signal_id like 'add_clustering%' then 'clustering'
            when ar.signal_id like 'materialize%' then 'materialization'
            when ar.signal_id like 'convert_to_incremental%' or ar.signal_id like 'apply_incremental%' then 'incremental'
            when ar.signal_id like 'expensive_query%' then 'expensive_query'
            else ar.domain
        end
    ) >= 2
)

select
    md5(node_id) as insight_id,
    table_fqn,
    node_id,
    model_name,
    project_name,
    warehouse_name,
    signals,
    signal_count,
    top_recommendation as primary_recommendation,
    top_domain as primary_domain,
    -- Root cause explanation
    case
        when has_materialization and has_spillage
            then 'View recomputation creates large intermediate results that spill — high warehouse impact'
        when has_incremental and has_spillage
            then 'Full table rebuilds overflow memory — high warehouse impact'
        when has_incremental and has_expensive_query
            then 'Query is expensive because it rebuilds the full table every run'
        when has_clustering and has_expensive_query
            then 'Expensive queries scan the full table because it lacks clustering'
        when has_clustering and has_spillage
            then 'Full table scans cause both poor pruning and memory overflow — high warehouse impact'
        when has_spillage and signal_count = 1
            then 'Spillage without other domain signals — warehouse capacity insufficient for workload'
        else 'Multiple optimization signals detected — compound inefficiency'
    end as root_cause,
    -- Action guidance
    case
        when has_materialization
            then 'Materialize the view — eliminates cascading recomputation'
        when has_incremental
            then 'Convert to incremental — smaller working set resolves secondary issues'
        when has_clustering
            then 'Add clustering key — reduces scan volume and downstream compute'
        when has_spillage
            then 'Scale up warehouse — no structural optimization available'
        else 'Investigate query patterns for root cause'
    end as recommended_action,
    snapshot_date
from signals_per_model
order by signal_count desc, top_priority
