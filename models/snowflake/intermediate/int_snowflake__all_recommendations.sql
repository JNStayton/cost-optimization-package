{{
  config(
    materialized='table',
  )
}}

{#--
  Unified recommendation surface that normalizes recommendations from all domain-specific
  fact models into a single interface with cost estimation, effort classification,
  signal identification, and priority tier assignment.

  This intermediate model is the shared foundation for all gold-layer views.
  Each gold view selects from this model with different filters/aggregations.

  Priority logic (v1 — stateless):
    - Rule 1: Co-signal deferral (blocking P2 model signals defer P3 warehouse config)
    - Rule 2: SQL refactor non-blocking (if only sql_refactor remains, promote P3→P2)
    - Rule 3: Savings-based promotion (P2→P1 when savings >= threshold)
  See docs/snowflake/optimization_priorities_mapping.md for full mapping.

  Grain: one row per (table_fqn or warehouse_name, domain, recommendation)
  Sources: warehouse config, spillage, expensive queries, materialization v2,
           incremental candidates, incremental config, clustering, AI spend
--#}

{% set credit_rate_usd = var('credit_rate_usd', 2) %}
{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

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
    -- WAREHOUSE CONFIG RECOMMENDATIONS
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
        'config_change' as effort_category,
        ws.total_credits_30d as score,
        ws.total_credits_30d * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        case
            when ws.symptom = 'idle_credit_consumption'
                then ws.total_idle_credits_30d * 12 * {{ credit_rate_usd }}
            when ws.symptom = 'oversized'
                then ws.total_idle_credits_30d * 12 * {{ credit_rate_usd }}
            else ws.total_credits_30d * 0.10 * 12 * {{ credit_rate_usd }}
        end as estimated_annual_savings_usd,
        ws.snowflake_ddl,
        ws.snapshot_date,
        case
            when ws.recommendation like '%Stable%' then 'stable'
            when ws.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        ws.recommendation_key as signal_id
    from {{ ref('fct_snowflake__warehouse_config_recommendations') }} as ws
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
        (sp.total_gb_spilled_local * 0.5 + sp.total_gb_spilled_remote * 5.0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        (sp.total_gb_spilled_local * 0.5 + sp.total_gb_spilled_remote * 5.0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} * 0.7 as estimated_annual_savings_usd,
        sp.snowflake_ddl,
        sp.snapshot_date,
        case
            when sp.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        case
            when sp.recommendation like '%Scale up%' then 'spillage_scale_up'
            when sp.recommendation like '%moderate%' and sp.recommendation like '%worse%' then 'spillage_moderate_worsening'
            when sp.recommendation like '%Stable%' or sp.recommendation like '%minor%' then 'spillage_moderate_stable'
            else 'spillage_scale_up'
        end as signal_id
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    left join warehouse_rates as wr on wr.warehouse_name = sp.warehouse_name
    where sp.recommendation not like 'Not available%'

    union all

    -- =========================================================================
    -- WAREHOUSE AGGREGATE SPILLAGE (warehouse-level signal)
    -- When total spillage across all models on a warehouse exceeds threshold,
    -- emit a warehouse-level scale-up recommendation even if no single model
    -- individually qualifies as "heavy."
    -- =========================================================================
    select
        'warehouse' as domain,
        sp_agg.warehouse_name as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        sp_agg.warehouse_name,
        'Scale up warehouse (aggregate spillage across models)' as recommendation,
        sp_agg.models_spilling || ' model(s) collectively spilling '
            || sp_agg.total_gb_spilled || ' GB over 30 days on ' || sp_agg.warehouse_name
            || '. No single model exceeds the heavy threshold, but the aggregate load indicates '
            || 'the warehouse is undersized for the combined workload.' as recommendation_reason,
        'config_change' as effort_category,
        sp_agg.total_gb_spilled as score,
        sp_agg.total_gb_spilled * 0.5
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        sp_agg.total_gb_spilled * 0.5
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} * 0.7 as estimated_annual_savings_usd,
        'ALTER WAREHOUSE ' || sp_agg.warehouse_name || ' SET WAREHOUSE_SIZE = '''
            || case sp_agg.warehouse_current_size
                when 'X-Small' then 'SMALL'
                when 'Small' then 'MEDIUM'
                when 'Medium' then 'LARGE'
                when 'Large' then 'XLARGE'
                when 'X-Large' then '2X-LARGE'
                else 'MEDIUM'
            end || ''';' as snowflake_ddl,
        current_date() as snapshot_date,
        'actionable' as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        'spillage_scale_up' as signal_id
    from (
        select
            sp.warehouse_name,
            count(distinct sp.table_fqn) as models_spilling,
            round(sum(sp.total_gb_spilled_local + sp.total_gb_spilled_remote), 2) as total_gb_spilled,
            max(wc.current_size) as warehouse_current_size
        from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
        left join {{ ref('int_snowflake__warehouse_config') }} as wc
            on wc.warehouse_name = sp.warehouse_name
        where sp.recommendation not like 'Not available%'
          and sp.recommendation not like 'Stable%'
        group by sp.warehouse_name
        having sum(sp.total_gb_spilled_local + sp.total_gb_spilled_remote) >= {{ var('spillage_aggregate_threshold_gb', 100) }}
            and max(case when sp.total_gb_spilled_local > 50 then 1 else 0 end) = 0
    ) as sp_agg
    left join warehouse_rates as wr on wr.warehouse_name = sp_agg.warehouse_name

    union all

    -- =========================================================================
    -- EXPENSIVE QUERIES
    -- =========================================================================
    select
        'warehouse' as domain,
        eq.query_hash as entity_name,
        dr_eq.table_fqn as table_fqn,
        eq.dbt_node_id as dbt_model,
        dr_eq.model_name as model_name,
        eq.warehouse_name,
        eq.recommendation,
        eq.recommendation_reason,
        'sql_refactor' as effort_category,
        eq.total_credits_30d as score,
        eq.estimated_annual_cost_usd,
        eq.estimated_annual_cost_usd * 0.20 as estimated_annual_savings_usd,
        null as snowflake_ddl,
        eq.snapshot_date,
        case
            when eq.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        case
            when eq.recommendation like '%Monitor%' then 'expensive_query_monitor'
            else 'expensive_query_actionable'
        end as signal_id
    from {{ ref('fct_snowflake__expensive_query_recommendations') }} as eq
    left join {{ ref('int_dbt__relations') }} as dr_eq
        on dr_eq.dbt_model = eq.dbt_node_id

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
        tm.select_count * tm.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        (tm.select_count - 1) * tm.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        tm.snapshot_date,
        case
            when tm.recommendation like '%Monitor%' then 'monitor'
            else 'actionable'
        end as backlog_status,
        '{% raw %}{{ config(materialized=''table'') }}{% endraw %}' as dbt_model_config,
        null as identified_unique_key,
        'materialize_as_table' as signal_id
    from {{ ref('fct_snowflake__table_materialization_candidates') }} as tm
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    where tm.recommendation != 'Monitor'
      {% if var('suppress_staging_materialization_recs', false) %}
      and not (
          lower(tm.model_name) like 'stg\_%' escape '\\'
          or lower(tm.model_name) like 'stage\_%' escape '\\'
          or lower(tm.model_name) like 'staging\_%' escape '\\'
          or lower(tm.schema_name) like '%staging%'
      )
      {% endif %}

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
        case
            when ic.recommendation like 'Strong%' then 'Convert to incremental materialization (strong signal)'
            when ic.recommendation like 'Good%' then 'Convert to incremental materialization (good signal)'
            else 'Evaluate incremental materialization'
        end as recommendation,
        ic.recommendation_reason,
        case
            when icr_lookup.recommendation_status = 'actionable_review' then 'actionable_review'
            when icr_lookup.recommendation_status = 'investigate' then 'investigation'
            else 'investigation'
        end as effort_category,
        ic.rebuild_pressure_score as score,
        ic.avg_build_time_sec * ic.builds_per_day * 365
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        ic.avg_build_time_sec * ic.builds_per_day * 365
            * coalesce(ic.rebuild_redundancy_rate, 0.5)
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        ic.snapshot_date,
        case
            when icr_lookup.recommendation_status = 'actionable_review' then 'actionable'
            when icr_lookup.recommendation_status = 'investigate' then 'monitor'
            when icr_lookup.recommendation_status = 'do_not_recommend' then 'stable'
            when ic.recommendation like '%Insufficient%' then 'monitor'
            when ic.roi_tier = 'low' then 'stable'
            else 'monitor'
        end as backlog_status,
        icr_lookup.dbt_model_config as dbt_model_config,
        icr_lookup.identified_unique_key,
        'convert_to_incremental' as signal_id
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }} as ic
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    left join {{ ref('fct_snowflake__incremental_config_recommendations') }} as icr_lookup
        on icr_lookup.table_fqn = ic.table_fqn
    where ic.recommendation not like '%Insufficient%'
      and ic.roi_tier != 'low'
      and coalesce(icr_lookup.recommendation_status, 'investigate') != 'do_not_recommend'
      -- Exclude when a specific strategy exists (apply_incremental_* will surface instead)
      and icr_lookup.incremental_strategy is null

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
        icr.strategy_notes as recommendation_reason,
        icr.effort_category,
        icr.table_size_gb as score,
        icr.avg_build_time_sec * coalesce(icr.builds_per_day, 1) * 365
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        icr.avg_build_time_sec * coalesce(icr.builds_per_day, 1) * 365
            * coalesce(icr.rebuild_redundancy_rate, 0.5)
            * coalesce(wr.credits_per_second, 0.000278)
            * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        icr.snapshot_date,
        case
            when icr.recommendation_status = 'actionable_review' then 'actionable'
            when icr.recommendation_status = 'investigate' then 'monitor'
            else 'stable'
        end as backlog_status,
        icr.dbt_model_config,
        icr.identified_unique_key,
        'apply_incremental_' || icr.incremental_strategy as signal_id
    from {{ ref('fct_snowflake__incremental_config_recommendations') }} as icr
    left join warehouse_rates as wr on wr.warehouse_name = (
        select warehouse_name from warehouse_rates order by credits_per_second desc limit 1
    )
    where icr.recommendation_status != 'do_not_recommend'
      and icr.incremental_strategy is not null

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
        case
            when tc.recommendation_tier = 'Strong' then 'Add clustering key (strong signal)'
            when tc.recommendation_tier = 'Good' then 'Add clustering key (good signal)'
            else 'Evaluate clustering key'
        end as recommendation,
        tc.recommendation_reason,
        'config_change' as effort_category,
        tc.score,
        tc.select_count * tc.avg_query_duration_s
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        tc.select_count * tc.avg_query_duration_s
            * greatest(tc.scan_ratio_pct / 100.0 - 0.2, 0)
            * coalesce(wr.credits_per_second, 0.000278)
            * 12 * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        tc.snapshot_date,
        'actionable' as backlog_status,
        case
            when tc.clustering_key is not null
                then '{% raw %}{{ config(cluster_by=[{% endraw %}' || '''' || replace(tc.clustering_key, ', ', ''', ''') || '''' || '{% raw %}]) }}{% endraw %}'
            else null
        end as dbt_model_config,
        null as identified_unique_key,
        case
            when tc.recommendation_tier = 'Strong' then 'add_clustering_key_strong'
            when tc.recommendation_tier = 'Good' then 'add_clustering_key_good'
            else 'add_clustering_key_evaluate'
        end as signal_id
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
        '-- Review AI usage patterns and consider model downgrades or batch processing' as snowflake_ddl,
        ai.snapshot_date,
        case
            when ai.wow_trend in ('Growing', 'New') then 'monitor'
            else 'stable'
        end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        'ai_spend_' || lower(ai.wow_trend) as signal_id
    from {{ ref('fct_snowflake__ai_spend_overview') }} as ai
    where ai.wow_trend in ('Growing', 'New')

    union all

    -- =========================================================================
    -- AI MODEL COST RECOMMENDATIONS
    -- =========================================================================
    select
        'ai' as domain,
        amc.model_name || '/' || coalesce(amc.function_name, 'all') as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        null as warehouse_name,
        amc.recommendation,
        amc.recommendation_reason,
        case
            when amc.recommendation like '%cheaper model%' then 'config_change'
            when amc.recommendation like '%prompt%' then 'sql_refactor'
            else 'config_change'
        end as effort_category,
        amc.total_credits as score,
        amc.projected_annual_cost_usd as estimated_annual_cost_usd,
        case
            when amc.recommendation like '%cheaper model%' then amc.projected_annual_cost_usd * 0.50
            when amc.recommendation like '%prompt%' then amc.projected_annual_cost_usd * 0.30
            when amc.recommendation like '%caching%' then amc.projected_annual_cost_usd * 0.20
            else null
        end as estimated_annual_savings_usd,
        '-- Review model usage: ' || amc.model_name || ' / ' || coalesce(amc.function_name, 'all') as snowflake_ddl,
        amc.snapshot_date,
        case when amc.recommendation like '%Monitor%' then 'stable' else 'actionable' end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        'ai_model_cost' as signal_id
    from {{ ref('fct_snowflake__ai_model_cost_recommendations') }} as amc
    where amc.recommendation not like '%Monitor%'

    union all

    -- =========================================================================
    -- AI TOKEN EFFICIENCY RECOMMENDATIONS
    -- =========================================================================
    select
        'ai' as domain,
        ate.model_name || '/' || coalesce(ate.function_name, 'all') || '/' || coalesce(ate.query_pattern, 'untagged') as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        null as warehouse_name,
        ate.recommendation,
        ate.recommendation_reason,
        case
            when ate.recommendation like '%failure%' then 'config_change'
            when ate.recommendation like '%cache%' then 'config_change'
            else 'sql_refactor'
        end as effort_category,
        ate.total_credits as score,
        ate.projected_annual_cost_usd as estimated_annual_cost_usd,
        case
            when ate.recommendation like '%failure%' then ate.projected_annual_cost_usd * ate.incomplete_pct / 100.0
            when ate.recommendation like '%cache%' then ate.projected_annual_cost_usd * 0.20
            when ate.recommendation like '%ratio%' or ate.recommendation like '%prompt%' then ate.projected_annual_cost_usd * 0.30
            else null
        end as estimated_annual_savings_usd,
        '-- Review token efficiency: ' || ate.model_name || ' / ' || coalesce(ate.query_pattern, 'untagged') as snowflake_ddl,
        ate.snapshot_date,
        case when ate.recommendation like '%efficient%' then 'stable' else 'actionable' end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        'ai_token_efficiency' as signal_id
    from {{ ref('fct_snowflake__ai_token_efficiency_recommendations') }} as ate
    where ate.recommendation not like '%efficient%'

    union all

    -- =========================================================================
    -- AI AGENT OPTIMIZATION RECOMMENDATIONS
    -- =========================================================================
    select
        'ai' as domain,
        aao.agent_fqn as entity_name,
        null as table_fqn,
        null as dbt_model,
        null as model_name,
        null as warehouse_name,
        aao.recommendation,
        aao.recommendation_reason,
        case
            when aao.recommendation like '%rapidly%' then 'config_change'
            when aao.recommendation like '%consolidate%' then 'architecture'
            else 'sql_refactor'
        end as effort_category,
        aao.total_credits_30d as score,
        aao.projected_annual_cost_usd as estimated_annual_cost_usd,
        case
            when aao.recommendation like '%rapidly%' then aao.projected_annual_cost_usd * 0.25
            when aao.recommendation like '%consolidate%' then aao.projected_annual_cost_usd * 0.80
            when aao.recommendation like '%per-request%' then aao.projected_annual_cost_usd * 0.30
            else null
        end as estimated_annual_savings_usd,
        '-- Review agent: ' || aao.agent_fqn as snowflake_ddl,
        aao.snapshot_date,
        case when aao.recommendation like '%Healthy%' then 'stable' else 'actionable' end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        'ai_agent_optimization' as signal_id
    from {{ ref('fct_snowflake__ai_agent_optimization_recommendations') }} as aao
    where aao.recommendation not like '%Healthy%'
),

-- Enrich with cross-environment data and warehouse fallback
enriched as (
    select
        ar.domain,
        ar.entity_name,
        ar.table_fqn,
        ar.dbt_model,
        ar.model_name,
        coalesce(ar.warehouse_name, build_wh.build_warehouse_name) as warehouse_name,
        ar.recommendation,
        ar.recommendation_reason,
        ar.effort_category,
        ar.score,
        ar.estimated_annual_cost_usd,
        ar.estimated_annual_savings_usd,
        ar.snowflake_ddl,
        ar.snapshot_date,
        ar.backlog_status,
        ar.dbt_model_config,
        ar.identified_unique_key,
        ar.signal_id,
        coalesce(rh.node_id, ar.dbt_model) as node_id,
        coalesce(rh.project_name, split_part(ar.dbt_model, '.', 2), wh_project.warehouse_project_name) as node_project_name,
        coalesce(rh.model_name, ar.model_name) as node_model_name,
        coalesce(rh.target_name, rh_fallback.target_name) as target_name,
        coalesce(rh.dbt_cloud_environment_id, rh_fallback.dbt_cloud_environment_id) as dbt_cloud_environment_id
    from all_recommendations as ar
    left join {{ ref('int_snowflake__dbt_relation_history') }} as rh
        on rh.table_fqn = ar.table_fqn
    -- Fallback: match on node_id when table_fqn doesn't match
    left join (
        select node_id, max(target_name) as target_name, max(dbt_cloud_environment_id) as dbt_cloud_environment_id
        from {{ ref('int_snowflake__dbt_relation_history') }}
        group by node_id
    ) as rh_fallback
        on rh_fallback.node_id = ar.dbt_model
        and rh.dbt_cloud_environment_id is null
    -- Warehouse fallback: most common warehouse that built this model (from query_history comments)
    left join (
        select
            parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string as node_id,
            mode(warehouse_name) as build_warehouse_name
        from {{ ref('int_snowflake__query_history') }}
        where query_text like '%node_id%'
            and query_start_time >= dateadd(day, -30, current_timestamp())
            and warehouse_name is not null
        group by 1
    ) as build_wh
        on build_wh.node_id = ar.dbt_model
        and ar.warehouse_name is null
    -- Warehouse-to-project mapping: for warehouse-level recs that have no model association
    -- Checks if the warehouse has been used by any monitored project (not MODE across all projects)
    left join (
        select distinct
            warehouse_name,
            '{{ monitored_projects[0] }}' as warehouse_project_name
        from {{ ref('int_snowflake__query_history') }}
        where query_text like '%node_id%'
            and query_start_time >= dateadd(day, -30, current_timestamp())
            and warehouse_name is not null
            and split_part(
                parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string,
                '.', 2
            ) in (
                {%- for proj in monitored_projects -%}
                    '{{ proj }}'{% if not loop.last %}, {% endif %}
                {%- endfor -%}
            )
    ) as wh_project
        on wh_project.warehouse_name = ar.warehouse_name
        and ar.dbt_model is null
),

-- =========================================================================
-- PRIORITY TIER LOGIC — Per-entity relative ordering
-- Priority is determined by hierarchy rank within each entity (model/warehouse).
-- A signal alone on an entity is P1. Co-occurring signals are ranked by hierarchy.
-- =========================================================================

prioritized as (
    select
        e.*,
        coalesce(e.node_id, e.entity_name) as dedup_key,
        -- Fixed hierarchy rank per signal type (lower = do first)
        case
            -- Rank 1: Always-safe warehouse config (no dependencies)
            when e.signal_id in (
                'idle_reduce_auto_suspend', 'idle_switch_scaling_policy',
                'idle_reduce_max_clusters', 'idle_reduce_min_clusters',
                'idle_enable_mcw_bursty',
                'provisioning_enable_auto_resume', 'provisioning_increase_suspend',
                'provisioning_increase_suspend_300', 'provisioning_warm_cluster',
                'overload_switch_scaling_policy'
            ) then 1
            -- Rank 2: Incremental/materialization (root cause — reduces rebuild waste)
            when e.signal_id in ('convert_to_incremental', 'materialize_as_table')
                or e.signal_id like 'apply_incremental_%'
                then 2
            -- Rank 3: Clustering (reduces scan width — do after incremental)
            when e.signal_id in (
                'add_clustering_key_strong', 'add_clustering_key_good',
                'add_clustering_key_evaluate'
            ) then 3
            -- Rank 4: Conditional warehouse config (deferred behind model fixes)
            when e.signal_id in (
                'spillage_scale_up', 'overload_enable_mcw', 'overload_scale_up_standard',
                'overload_increase_clusters', 'overload_scale_up_large_mcw',
                'oversized_scale_down', 'oversized_disable_mcw',
                'overload_at_max_standard', 'idle_consolidate_standard',
                'idle_consolidate_underloaded', 'provisioning_gen2'
            ) then 4
            -- Rank 5: Monitor/investigate signals
            when e.signal_id in (
                'spillage_moderate_worsening', 'spillage_moderate_stable',
                'expensive_query_monitor', 'expensive_query_actionable'
            ) then 5
            -- Rank 6: AI domain
            when e.domain = 'ai' then 6
            -- Default
            else 5
        end as hierarchy_rank,
        -- Per-entity priority: rank 1 = do first for this entity
        -- Actionable items always rank above monitor/stable regardless of hierarchy
        row_number() over (
            partition by coalesce(e.node_id, e.entity_name)
            order by
                case e.backlog_status
                    when 'actionable' then 1
                    when 'monitor' then 2
                    else 3
                end,
                case
                    when e.signal_id in (
                        'idle_reduce_auto_suspend', 'idle_switch_scaling_policy',
                        'idle_reduce_max_clusters', 'idle_reduce_min_clusters',
                        'idle_enable_mcw_bursty',
                        'provisioning_enable_auto_resume', 'provisioning_increase_suspend',
                        'provisioning_increase_suspend_300', 'provisioning_warm_cluster',
                        'overload_switch_scaling_policy'
                    ) then 1
                    when e.signal_id in ('convert_to_incremental', 'materialize_as_table')
                        or e.signal_id like 'apply_incremental_%'
                        then 2
                    when e.signal_id in (
                        'add_clustering_key_strong', 'add_clustering_key_good',
                        'add_clustering_key_evaluate'
                    ) then 3
                    when e.signal_id in (
                        'spillage_scale_up', 'overload_enable_mcw', 'overload_scale_up_standard',
                        'overload_increase_clusters', 'overload_scale_up_large_mcw',
                        'oversized_scale_down', 'oversized_disable_mcw',
                        'overload_at_max_standard', 'idle_consolidate_standard',
                        'idle_consolidate_underloaded', 'provisioning_gen2'
                    ) then 4
                    when e.signal_id in (
                        'spillage_moderate_worsening', 'spillage_moderate_stable',
                        'expensive_query_monitor', 'expensive_query_actionable'
                    ) then 5
                    when e.domain = 'ai' then 6
                    else 5
                end,
                e.estimated_annual_savings_usd desc nulls last
        ) as priority_tier
    from enriched as e
)

select
    domain,
    entity_name,
    table_fqn,
    dbt_model,
    model_name,
    warehouse_name,
    recommendation,
    recommendation_reason,
    effort_category,
    score,
    estimated_annual_cost_usd,
    estimated_annual_savings_usd,
    snowflake_ddl,
    snapshot_date,
    backlog_status,
    dbt_model_config,
    identified_unique_key,
    signal_id,
    hierarchy_rank,
    priority_tier,
    node_id,
    node_project_name,
    node_model_name,
    target_name,
    dbt_cloud_environment_id,
    dedup_key
from prioritized
where {{ scope_filter('node_project_name', 'node_id') }}
