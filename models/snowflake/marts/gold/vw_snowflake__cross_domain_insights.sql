{{
  config(
    materialized='view',
  )
}}

{#--
  Cross-domain correlation insights. Detects cases where recommendations from
  different domains point to the same root cause or compound each other.

  Each row represents a detected correlation between 2+ domain signals on the
  same table/model, with a recommended fix order and combined savings estimate.
--#}

with spillage_tables as (
    select table_fqn, dbt_model, model_name, warehouse_name,
           total_gb_spilled_local, total_gb_spilled_remote, recommendation
    from {{ ref('fct_snowflake__warehouse_spillage_recommendations') }}
    where recommendation not like 'Not available%'
),

clustering_tables as (
    select table_fqn, dbt_model, score, scan_ratio_pct, select_count, clustering_key
    from {{ ref('fct_snowflake__table_clustering_candidates') }}
    where is_candidate = true
),

materialization_tables as (
    select table_fqn, dbt_model, model_name, materialization_score, select_count, avg_query_duration_s,
           is_in_view_chain, downstream_table_count
    from {{ ref('fct_snowflake__table_materialization_candidates_v2') }}
    where recommendation = 'Materialize as TABLE'
),

incremental_tables as (
    select table_fqn, dbt_model, model_name, compute_waste_score, table_size_gb,
           rebuild_redundancy_rate, avg_build_time_sec
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }}
    where recommendation not like '%Insufficient%'
),

expensive_queries as (
    select query_hash, dbt_node_id, warehouse_name, total_credits_30d, estimated_annual_cost_usd
    from {{ ref('fct_snowflake__expensive_query_recommendations') }}
),

sizing_recs as (
    select warehouse_name, recommendation, total_credits_30d, total_idle_credits_30d
    from {{ ref('fct_snowflake__warehouse_sizing_recommendations') }}
    where recommendation like 'Scale down%'
),

-- =========================================================================
-- Correlation 1: Spillage + Clustering Candidate
-- =========================================================================
corr_spillage_clustering as (
    select
        'spillage_plus_clustering' as insight_type,
        sp.table_fqn,
        sp.dbt_model,
        sp.model_name,
        array_construct('warehouse', 'clustering') as domains_involved,
        'Full table scans cause both poor pruning and memory overflow' as root_cause,
        'Cluster first — reduces scan volume and likely eliminates spillage' as recommended_fix_order,
        'Table has ' || round(ct.scan_ratio_pct, 0) || '% scan ratio AND '
            || round(sp.total_gb_spilled_local, 1) || ' GB spillage. '
            || 'Clustering on ' || coalesce(ct.clustering_key, 'recommended columns')
            || ' would reduce both.' as evidence
    from spillage_tables as sp
    inner join clustering_tables as ct on ct.table_fqn = sp.table_fqn
),

-- =========================================================================
-- Correlation 2: View in Chain + Downstream Spillage
-- =========================================================================
corr_view_spillage as (
    select
        'view_chain_plus_spillage' as insight_type,
        mt.table_fqn,
        mt.dbt_model,
        mt.model_name,
        array_construct('materialization', 'warehouse') as domains_involved,
        'View recomputation creates large intermediate results that spill downstream' as root_cause,
        'Materialize the view — eliminates cascading recomputation and spillage' as recommended_fix_order,
        'View ' || mt.model_name || ' has ' || mt.downstream_table_count
            || ' downstream table(s). Materializing eliminates repeated computation.' as evidence
    from materialization_tables as mt
    where mt.is_in_view_chain = true
      and exists (
          select 1 from spillage_tables sp
          where sp.table_fqn != mt.table_fqn
      )
),

-- =========================================================================
-- Correlation 5: Expensive Query + Incremental Candidate
-- =========================================================================
corr_expensive_incremental as (
    select
        'expensive_query_plus_incremental' as insight_type,
        ic.table_fqn,
        ic.dbt_model,
        ic.model_name,
        array_construct('warehouse', 'materialization') as domains_involved,
        'Query is expensive because it rebuilds the full table every run' as root_cause,
        'Convert to incremental — reduces working set and per-run cost' as recommended_fix_order,
        'Model ' || ic.model_name || ' costs credits monthly as an expensive query AND '
            || 'has ' || round(coalesce(ic.rebuild_redundancy_rate, 0) * 100, 0)
            || '% rebuild redundancy. Incremental would process only delta.' as evidence
    from incremental_tables as ic
    inner join expensive_queries as eq on eq.dbt_node_id = ic.dbt_model
),

-- =========================================================================
-- Correlation 6: Spillage + Incremental Candidate
-- =========================================================================
corr_spillage_incremental as (
    select
        'spillage_plus_incremental' as insight_type,
        sp.table_fqn,
        sp.dbt_model,
        sp.model_name,
        array_construct('warehouse', 'materialization') as domains_involved,
        'Full table rebuilds overflow memory because the entire dataset is processed' as root_cause,
        'Convert to incremental — smaller working set eliminates spillage' as recommended_fix_order,
        'Table spills ' || round(sp.total_gb_spilled_local, 1) || ' GB AND has '
            || round(coalesce(ic.rebuild_redundancy_rate, 0) * 100, 0)
            || '% rebuild redundancy. Incremental would reduce working set.' as evidence
    from spillage_tables as sp
    inner join incremental_tables as ic on ic.table_fqn = sp.table_fqn
),

-- =========================================================================
-- Correlation 9: Clustering Candidate + Expensive Query
-- =========================================================================
corr_clustering_expensive as (
    select
        'clustering_plus_expensive_query' as insight_type,
        ct.table_fqn,
        ct.dbt_model,
        null as model_name,
        array_construct('clustering', 'warehouse') as domains_involved,
        'Expensive queries scan the full table because it lacks clustering' as root_cause,
        'Add clustering key — reduces scan cost for expensive queries' as recommended_fix_order,
        'Table has ' || round(ct.scan_ratio_pct, 0) || '% scan ratio with '
            || ct.select_count || ' queries. Clustering on '
            || coalesce(ct.clustering_key, 'recommended columns') || ' would reduce cost.' as evidence
    from clustering_tables as ct
    where exists (
        select 1 from expensive_queries eq
        where eq.dbt_node_id = ct.dbt_model
    )
),

-- =========================================================================
-- Correlation 10: Oversized Warehouse + Low Query Volume
-- =========================================================================
corr_oversized_idle as (
    select
        'oversized_warehouse_idle' as insight_type,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        array_construct('warehouse') as domains_involved,
        'Warehouse is barely used but stays running — auto-suspend too lenient' as root_cause,
        'Reduce auto-suspend timeout first, then evaluate sizing' as recommended_fix_order,
        'Warehouse ' || sr.warehouse_name || ' is oversized with '
            || round(coalesce(sr.total_idle_credits_30d, 0), 1) || ' idle credits/month. '
            || 'Reduce auto-suspend to 60s.' as evidence
    from sizing_recs as sr
    where coalesce(sr.total_idle_credits_30d, 0) > 5
),

-- =========================================================================
-- UNION ALL CORRELATIONS
-- =========================================================================
all_insights as (
    select * from corr_spillage_clustering
    union all
    select * from corr_view_spillage
    union all
    select * from corr_expensive_incremental
    union all
    select * from corr_spillage_incremental
    union all
    select * from corr_clustering_expensive
    union all
    select * from corr_oversized_idle
)

select
    md5(insight_type || '|' || coalesce(table_fqn, '') || '|' || coalesce(dbt_model, '')) as insight_id,
    insight_type,
    table_fqn,
    dbt_model as node_id,
    model_name,
    domains_involved,
    root_cause,
    recommended_fix_order,
    evidence,
    current_date() as snapshot_date
from all_insights
