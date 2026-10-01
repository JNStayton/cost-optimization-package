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

  view_chain is a signal of its own: the model is a table whose builds recompute
  upstream views and ephemerals inline (int_snowflake__table_upstream_views). It
  counts toward the 2-signal minimum. With spillage or an expensive query, the
  recommended action keeps the model's own action and adds the chain's recommended
  view to materialize.

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
    where ar.node_id is not null
      and (
          -- Clustering signals only count when actionable (has a recommended key)
          (ar.signal_id like 'add_clustering%' and ar.backlog_status = 'actionable')
          or (ar.signal_id not like 'add_clustering%' and ar.backlog_status in ('actionable', 'monitor'))
      )
    group by ar.node_id, coalesce(ar.node_model_name, ar.model_name), ar.table_fqn, ar.node_project_name
),

with_chain as (
    select
        spm.* exclude (signals, signal_count),
        tuv.node_id is not null                                         as has_view_chain,
        tuv.upstream_view_chain,
        tuv.upstream_view_count,
        tuv.chain_recommended_view,
        spm.signal_count                                                as own_signal_count,
        iff(tuv.node_id is not null, array_append(spm.signals, 'view_chain'), spm.signals) as signals,
        spm.signal_count + iff(tuv.node_id is not null, 1, 0)           as signal_count,
        tuv.upstream_view_count || ' upstream view(s)'                  as chain_phrase
    from signals_per_model as spm
    left join (
        select table_dbt_model as node_id, upstream_view_chain, upstream_view_count, chain_recommended_view
        from {{ ref('int_snowflake__table_upstream_views') }}
        where table_dbt_model is not null
    ) as tuv
        on tuv.node_id = spm.node_id
),

actions as (
    select
        wc.*,
        -- The model's own top action, in hierarchy order
        case
            when has_materialization
                then 'Materialize the view — eliminates cascading recomputation'
            when has_incremental
                then 'Convert to incremental — smaller working set resolves secondary issues'
            when has_clustering
                then 'Add clustering key — reduces scan volume and downstream compute'
        end as own_action,
        case
            when has_spillage then 'which adds to its spill'
            when has_expensive_query then 'which adds to its cost'
        end as chain_effect,
        -- Combination root cause from the model's own signals (null when fewer than 2)
        case
            when own_signal_count < 2 then null
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
            else 'Multiple optimization signals detected — compound inefficiency'
        end as own_root_cause,
        case
            when has_spillage then 'Builds recompute ' || chain_phrase || ' inline, enlarging the working set that spills'
            when has_expensive_query then 'Builds are expensive partly because they recompute ' || chain_phrase
            else 'Builds recompute ' || chain_phrase || ' inline'
        end as chain_root_cause
    from with_chain as wc
    where signal_count >= 2
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
    upstream_view_chain,
    upstream_view_count,
    chain_recommended_view,
    -- Root cause explanation: the model's own combination, plus the chain's part
    case
        when not has_view_chain then own_root_cause
        when own_root_cause is null then chain_root_cause
        when chain_effect is not null then own_root_cause || '. ' || chain_root_cause
        else own_root_cause
    end as root_cause,
    -- Action guidance: the model's own top action; with spillage or an expensive query,
    -- plus the view chain's recommended view to materialize
    case
        when has_view_chain and chain_effect is not null and own_action is not null
            then own_action || '; additionally, '
                || coalesce('materialize ' || chain_recommended_view || ' as a table: this model''s builds recompute '
                                || chain_phrase || ', ' || chain_effect,
                            'its builds recompute ' || chain_phrase || ', ' || chain_effect
                                || ' (see vw_snowflake__dbt_model_optimizations)')
        when has_view_chain and chain_effect is not null
            then coalesce('Materialize ' || chain_recommended_view || ' as a table: this model''s builds recompute '
                              || chain_phrase || ', ' || chain_effect,
                          'Its builds recompute ' || chain_phrase || ', ' || chain_effect
                              || '. See vw_snowflake__dbt_model_optimizations')
        when own_action is not null then own_action
        when has_spillage
            then 'Scale up warehouse — no structural optimization available'
        else 'Investigate query patterns for root cause'
    end as recommended_action,
    snapshot_date
from actions
order by signal_count desc, top_priority
