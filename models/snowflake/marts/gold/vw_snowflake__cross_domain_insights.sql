{{
  config(
    materialized='view',
  )
}}

{#--
  Cross-domain signal detection. For each table with 2+ optimization signals
  from different domains, surfaces the signals as an array and picks the
  primary recommendation using a fixed hierarchy:

    1. Materialize as table (resolves view chain + downstream cascading)
    2. Convert to incremental (resolves rebuild waste + spillage from large rebuilds)
    3. Add clustering (resolves scan inefficiency)
    4. Resize warehouse / reduce spillage (config-level fix)
    5. Refactor SQL (highest effort, last resort)

  Only shows entities where at least one signal is P1 (actionable now).
  One row per table_fqn — naturally deduped.
--#}

{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

with base_tables as (
    select table_fqn, dbt_model as node_id, model_name, package_name as project_name
    from {{ ref('int_dbt__relations') }}
    where package_name in (
        {% for proj in monitored_projects %}
          '{{ proj }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
),

spillage_set as (
    select distinct table_fqn
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }}
    where recommendation not like 'Not available%'
),

clustering_set as (
    select distinct table_fqn
    from {{ ref('fct_snowflake__table_clustering_candidates') }}
    where is_candidate = true
),

materialization_set as (
    select distinct table_fqn, is_in_view_chain
    from {{ ref('fct_snowflake__table_materialization_candidates_v2') }}
    where recommendation = 'Materialize as TABLE'
),

incremental_set as (
    select distinct ic.table_fqn
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }} as ic
    left join {{ ref('fct_snowflake__incremental_config_recommendations') }} as icr
        on icr.table_fqn = ic.table_fqn
    where ic.recommendation not like '%Insufficient%'
      and ic.recommendation != 'Low ROI — Minimal Rebuild Redundancy'
      and ic.roi_tier != 'low'
      and coalesce(icr.recommendation_status, 'investigate') != 'do_not_recommend'
),

expensive_set as (
    select distinct dbt_node_id as node_id
    from {{ ref('fct_snowflake__expensive_query_recommendations') }}
),

signals_joined as (
    select
        bt.table_fqn,
        bt.node_id,
        bt.model_name,
        bt.project_name,
        sp.table_fqn is not null as has_spillage,
        cl.table_fqn is not null as has_clustering_candidate,
        mt.table_fqn is not null as has_materialization_candidate,
        coalesce(mt.is_in_view_chain, false) as is_in_view_chain,
        ic.table_fqn is not null as has_incremental_candidate,
        eq.node_id is not null as has_expensive_query,
        -- Signal count
        (iff(sp.table_fqn is not null, 1, 0)
         + iff(cl.table_fqn is not null, 1, 0)
         + iff(mt.table_fqn is not null, 1, 0)
         + iff(ic.table_fqn is not null, 1, 0)
         + iff(eq.node_id is not null, 1, 0)
         + iff(coalesce(mt.is_in_view_chain, false), 1, 0)
        ) as signal_count,
        -- Signals array
        array_construct_compact(
            iff(sp.table_fqn is not null, 'spillage', null),
            iff(cl.table_fqn is not null, 'clustering', null),
            iff(mt.table_fqn is not null, 'materialization', null),
            iff(coalesce(mt.is_in_view_chain, false), 'view_chain', null),
            iff(ic.table_fqn is not null, 'incremental', null),
            iff(eq.node_id is not null, 'expensive_query', null)
        ) as signals
    from base_tables as bt
    left join spillage_set as sp on sp.table_fqn = bt.table_fqn
    left join clustering_set as cl on cl.table_fqn = bt.table_fqn
    left join materialization_set as mt on mt.table_fqn = bt.table_fqn
    left join incremental_set as ic on ic.table_fqn = bt.table_fqn
    left join expensive_set as eq on eq.node_id = bt.node_id
),

multi_signal_tables as (
    select * from signals_joined where signal_count >= 2
),

-- Warehouse-level: oversized + idle
warehouse_insights as (
    select
        ws.warehouse_name,
        round(coalesce(ws.total_idle_credits_30d, 0), 1) as idle_credits
    from {{ ref('fct_snowflake__warehouse_config_recommendations') }} as ws
    where ws.recommendation like 'Scale down%'
      and coalesce(ws.total_idle_credits_30d, 0) > 5
)

select
    md5(mst.table_fqn || '|' || coalesce(mst.node_id, '')) as insight_id,
    mst.table_fqn,
    mst.node_id,
    mst.model_name,
    mst.project_name,
    null as warehouse_name,
    mst.signals,
    mst.signal_count,
    case
        when mst.has_materialization_candidate or mst.is_in_view_chain
            then 'Materialize as table'
        when mst.has_incremental_candidate
            then 'Convert to incremental'
        when mst.has_clustering_candidate
            then 'Add clustering key'
        when mst.has_spillage and mst.signal_count = 1
            then 'Upsize warehouse'
        else 'Investigate query patterns'
    end as primary_recommendation,
    case
        when mst.has_materialization_candidate or mst.is_in_view_chain then 'materialization'
        when mst.has_incremental_candidate then 'materialization'
        when mst.has_clustering_candidate then 'clustering'
        else 'warehouse'
    end as primary_domain,
    case
        when mst.is_in_view_chain and mst.has_spillage
            then 'View recomputation creates large intermediate results that spill — high warehouse impact'
        when mst.has_incremental_candidate and mst.has_spillage
            then 'Full table rebuilds overflow memory — high warehouse impact'
        when mst.has_incremental_candidate and mst.has_expensive_query
            then 'Query is expensive because it rebuilds the full table every run'
        when mst.has_clustering_candidate and mst.has_expensive_query
            then 'Expensive queries scan the full table because it lacks clustering'
        when mst.has_clustering_candidate and mst.has_spillage
            then 'Full table scans cause both poor pruning and memory overflow — high warehouse impact'
        when mst.has_spillage and mst.signal_count = 1
            then 'Spillage without other domain signals — warehouse capacity insufficient for workload'
        else 'Multiple optimization signals detected — compound inefficiency'
    end as root_cause,
    case
        when mst.has_materialization_candidate or mst.is_in_view_chain
            then 'Materialize the view — eliminates cascading recomputation'
        when mst.has_incremental_candidate
            then 'Convert to incremental — smaller working set resolves secondary issues'
        when mst.has_clustering_candidate
            then 'Add clustering key — reduces scan volume and downstream compute'
        when mst.has_spillage and mst.signal_count = 1
            then 'Upsize warehouse — no structural optimization available'
        else 'Investigate query patterns for root cause'
    end as recommended_action,
    current_date() as snapshot_date
from multi_signal_tables as mst

union all

select
    md5(wi.warehouse_name) as insight_id,
    null as table_fqn,
    null as node_id,
    null as model_name,
    null as project_name,
    wi.warehouse_name,
    array_construct('oversized', 'idle') as signals,
    2 as signal_count,
    'Reduce auto-suspend timeout' as primary_recommendation,
    'warehouse' as primary_domain,
    'Warehouse is oversized and mostly idle — auto-suspend too lenient' as root_cause,
    'Reduce auto-suspend timeout to 60s, then evaluate sizing' as recommended_action,
    current_date() as snapshot_date
from warehouse_insights as wi
