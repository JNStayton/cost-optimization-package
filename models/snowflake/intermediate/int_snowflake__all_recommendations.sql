{{
  config(
    materialized='view',
  )
}}

{#--
  Unified recommendation surface that normalizes recommendations from all domain-specific
  fact models into a single interface with cost estimation and effort classification.

  This intermediate model is the shared foundation for all gold-layer views.
  Each gold view selects from this model with different filters/aggregations.

  Grain: one row per (table_fqn or warehouse_name, domain, recommendation)
  Sources: warehouse sizing, spillage, expensive queries, materialization v2,
           incremental candidates, incremental config, clustering, AI spend
--#}

{% set credit_rate_usd = var('credit_rate_usd', 2) %}

with warehouse_rates as (
    -- Derive credits-per-second per warehouse from actual metering data
    select
        warehouse_name,
        avg(total_credits) / 3600.0 as credits_per_second
    from {{ ref('int_snowflake__warehouse_daily') }}
    where total_credits > 0
    group by warehouse_name
),

all_recommendations as (

    -- =========================================================================
    -- WAREHOUSE SIZING
    -- =========================================================================
    select
        'warehouse' as domain,
        ws.warehouse_name as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        ws.warehouse_name,
        ws.recommendation,
        ws.recommendation_reason,
        case
            when ws.recommendation like 'Scale down%' then 'config_change'
            when ws.recommendation like 'Enable Gen2%' then 'config_change'
            when ws.recommendation like 'Enable multi-cluster%' then 'config_change'
            when ws.recommendation like 'Scale up%' then 'config_change'
            else 'config_change'
        end as effort_category,
        ws.total_credits_30d as score,
        -- Cost estimation
        ws.total_credits_30d * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        case
            when ws.recommendation like 'Scale down%'
                then ws.total_idle_credits_30d * 12 * {{ credit_rate_usd }}
            when ws.recommendation like 'Enable Gen2%'
                then ws.total_credits_30d * 0.10 * 12 * {{ credit_rate_usd }}
            else null
        end as estimated_annual_savings_usd,
        -- Actionable SQL
        case
            when ws.recommendation like 'Scale down%'
                then 'ALTER WAREHOUSE ' || ws.warehouse_name || ' SET WAREHOUSE_SIZE = ''X-SMALL'';'
            when ws.recommendation like 'Enable Gen2%'
                then 'ALTER WAREHOUSE ' || ws.warehouse_name || ' SET RESOURCE_CONSTRAINT = ''STANDARD_GEN_2'';'
            when ws.recommendation like 'Enable multi-cluster%'
                then 'ALTER WAREHOUSE ' || ws.warehouse_name || ' SET MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3;'
            when ws.recommendation like 'Scale up%'
                then 'ALTER WAREHOUSE ' || ws.warehouse_name || ' SET WAREHOUSE_SIZE = ''MEDIUM'';'
            else null
        end as actionable_sql,
        ws.snapshot_date,
        case
            when ws.recommendation like '%Stable%' then 'stable'
            when ws.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__warehouse_sizing_recommendations') }} as ws
    where ws.recommendation not like 'Stable%'

    union all

    -- =========================================================================
    -- WAREHOUSE SPILLAGE
    -- =========================================================================
    select
        'warehouse' as domain,
        sp.table_fqn as entity_name,
        sp.table_fqn,
        sp.dbt_model,
        sp.model_name,
        sp.warehouse_name,
        sp.recommendation,
        sp.recommendation_reason,
        case
            when sp.total_gb_spilled_remote > 0 then 'sql_refactor'
            when sp.total_gb_spilled_local > 50 then 'sql_refactor'
            else 'config_change'
        end as effort_category,
        sp.total_gb_spilled_local + sp.total_gb_spilled_remote as score,
        -- Cost: estimate spill overhead as added query time
        (sp.total_gb_spilled_local * 0.5 + sp.total_gb_spilled_remote * 5.0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        (sp.total_gb_spilled_local * 0.5 + sp.total_gb_spilled_remote * 5.0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} * 0.7 as estimated_annual_savings_usd,
        case
            when sp.total_gb_spilled_remote > 0
                then 'ALTER WAREHOUSE ' || coalesce(sp.warehouse_name, '<warehouse>') || ' SET WAREHOUSE_SIZE = ''LARGE''; -- or refactor SQL'
            else '-- Review model SQL for wide joins, missing filters, or unnecessary columns'
        end as actionable_sql,
        sp.snapshot_date,
        case
            when sp.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__warehouse_spillage_recommendations') }} as sp
    left join warehouse_rates as wr on wr.warehouse_name = sp.warehouse_name
    where sp.recommendation not like 'Not available%'

    union all

    -- =========================================================================
    -- EXPENSIVE QUERIES
    -- =========================================================================
    select
        'warehouse' as domain,
        eq.query_hash as entity_name,
        null as table_fqn,
        eq.dbt_node_id as dbt_model,
        null as model_name,
        eq.warehouse_name,
        eq.recommendation,
        eq.recommendation_reason,
        'sql_refactor' as effort_category,
        eq.total_credits_30d as score,
        eq.estimated_annual_cost_usd,
        eq.estimated_annual_cost_usd * 0.20 as estimated_annual_savings_usd,
        '-- Profile query ' || eq.query_hash || ' and optimize SQL' as actionable_sql,
        eq.snapshot_date,
        case
            when eq.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__expensive_query_recommendations') }} as eq

    union all

    -- =========================================================================
    -- TABLE MATERIALIZATION (V2)
    -- =========================================================================
    select
        'materialization' as domain,
        tm.table_fqn as entity_name,
        tm.table_fqn,
        tm.dbt_model,
        tm.model_name,
        null as warehouse_name,
        tm.recommendation,
        tm.recommendation_reason,
        'config_change' as effort_category,
        tm.materialization_score as score,
        -- Cost: view recomputes on every SELECT
        tm.select_count * tm.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        (tm.select_count - 1) * tm.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        '{% raw %}{{ config(materialized=''table'') }}{% endraw %}' as actionable_sql,
        tm.snapshot_date,
        case
            when tm.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__table_materialization_candidates_v2') }} as tm
    -- Use the warehouse that queries this view most (approximation: use any available rate)
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    where tm.recommendation != 'Monitor'

    union all

    -- =========================================================================
    -- INCREMENTAL MATERIALIZATION CANDIDATES
    -- =========================================================================
    select
        'materialization' as domain,
        ic.table_fqn as entity_name,
        ic.table_fqn,
        ic.dbt_model,
        ic.model_name,
        null as warehouse_name,
        ic.recommendation,
        ic.recommendation_reason,
        case
            when ic.recommendation like 'Strong%' then 'architecture'
            else 'architecture'
        end as effort_category,
        ic.compute_waste_score as score,
        -- Cost: full rebuild every time
        ic.avg_build_time_sec * ic.builds_per_day * 365
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        -- Savings: proportional to redundancy rate
        ic.avg_build_time_sec * ic.builds_per_day * 365
            * coalesce(ic.rebuild_redundancy_rate, 0.5)
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        '-- Convert to incremental materialization (see incremental_config_recommendations for template)' as actionable_sql,
        ic.snapshot_date,
        case
            when ic.recommendation like '%Monitor%' or ic.recommendation like '%Insufficient%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }} as ic
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    where ic.recommendation not like '%Insufficient%'

    union all

    -- =========================================================================
    -- INCREMENTAL CONFIG RECOMMENDATIONS
    -- =========================================================================
    select
        'materialization' as domain,
        icr.table_fqn as entity_name,
        icr.table_fqn,
        icr.dbt_model,
        icr.model_name,
        null as warehouse_name,
        'Apply incremental config: ' || icr.incremental_strategy as recommendation,
        'Redundancy tier: ' || icr.redundancy_tier
            || '. Strategy: ' || icr.incremental_strategy
            || '. Unique key: ' || coalesce(icr.best_unique_key, 'none detected')
            || '.' as recommendation_reason,
        'config_change' as effort_category,
        icr.table_size_gb as score,
        icr.avg_build_time_sec * coalesce(icr.builds_per_day, 1) * 365
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        icr.avg_build_time_sec * coalesce(icr.builds_per_day, 1) * 365
            * coalesce(icr.rebuild_redundancy_rate, 0.5)
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        icr.dbt_config_template as actionable_sql,
        icr.snapshot_date,
        'actionable' as backlog_status,
        icr.dbt_config_template,
        icr.validate_uniqueness_sql
    from {{ ref('fct_snowflake__incremental_config_recommendations') }} as icr
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )

    union all

    -- =========================================================================
    -- TABLE CLUSTERING CANDIDATES
    -- =========================================================================
    select
        'clustering' as domain,
        tc.table_fqn as entity_name,
        tc.table_fqn,
        tc.dbt_model,
        null as model_name,
        null as warehouse_name,
        tc.recommendation_tier || ' clustering candidate' as recommendation,
        tc.recommendation_reason,
        'config_change' as effort_category,
        tc.score,
        -- Cost: full scans on every query
        tc.select_count * tc.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        -- Savings: proportional to scan ratio reduction (target 0.2)
        tc.select_count * tc.avg_query_duration_s
            * greatest(tc.scan_ratio_pct / 100.0 - 0.2, 0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        'ALTER TABLE ' || tc.table_fqn || ' CLUSTER BY (' || coalesce(tc.clustering_key, '<recommended_columns>') || ');' as actionable_sql,
        tc.snapshot_date,
        'actionable' as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__table_clustering_candidates') }} as tc
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    where tc.is_candidate = true

    union all

    -- =========================================================================
    -- AI SPEND OVERVIEW (service-level)
    -- =========================================================================
    select
        'ai' as domain,
        ai.service_type as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        null as warehouse_name,
        case
            when ai.wow_trend = 'Growing' then 'AI spend growing — review usage'
            when ai.wow_trend = 'New' then 'New AI service detected'
            else 'AI spend stable'
        end as recommendation,
        ai.service_type || ': ' || round(ai.total_credits, 2) || ' credits over '
            || ai.active_days || ' days. Trend: ' || ai.wow_trend || '.' as recommendation_reason,
        'config_change' as effort_category,
        ai.total_credits as score,
        ai.projected_annual_cost_usd as estimated_annual_cost_usd,
        case when ai.wow_trend = 'Growing' then ai.projected_annual_cost_usd * 0.15 else null end as estimated_annual_savings_usd,
        '-- Review AI usage patterns and consider model downgrades or batch processing' as actionable_sql,
        ai.snapshot_date,
        case
            when ai.wow_trend in ('Growing', 'New') then 'monitor'
            else 'stable'
        end as backlog_status,
        null as dbt_config_template,
        null as validate_uniqueness_sql
    from {{ ref('fct_snowflake__ai_spend_overview') }} as ai
    where ai.wow_trend in ('Growing', 'New')
),

-- Enrich with cross-environment data
enriched as (
    select
        ar.*,
        rh.node_id,
        rh.project_name as node_project_name,
        rh.model_name as node_model_name,
        rh.target_name,
        case
            when lower(rh.target_name) in ('prod', 'default', 'production', 'main')
                or lower(rh.target_name) like '%prod%' then 1
            when lower(rh.target_name) like '%stag%' then 2
            else 3
        end as env_priority
    from all_recommendations as ar
    left join {{ ref('int_snowflake__dbt_relation_history') }} as rh
        on rh.table_fqn = ar.table_fqn
)

select
    *,
    coalesce(node_id, entity_name) as dedup_key
from enriched
