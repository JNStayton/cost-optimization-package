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
{% set min_savings = var('min_annual_savings_usd', 1) %}
{#- Materialization and clustering counts cover their marts' lookback windows (same vars
    and defaults as those marts), so annualize by 365 / window, not x12 (a 30-day window). -#}
{% set annualize_materialization = 365.0 / var('table_materialization_lookback_days', 14) %}
{% set annualize_clustering = 365.0 / var('clustering_candidates_lookback_days', 7) %}
{% set spillage_lookback_days = var('spillage_lookback_days', 30) %}
{% set annualize_spillage = 365.0 / spillage_lookback_days %}
{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

with warehouse_list_rates as (
    -- Map warehouse size to Snowflake's published credits-per-hour rate.
    -- Used for forward-looking cost and savings estimates (not attribution).
    select
        warehouse_name,
        {{ warehouse_credits_per_hour('current_size') }} as credits_per_hour
    from {{ ref('int_snowflake__warehouse_config') }}
),

model_warehouses as (
    -- Most common warehouse per dbt model (node_id), from query comment parsing.
    -- Used to price table-level recommendations at the model's actual build warehouse
    -- and to resolve warehouse_name in enriched for model-level recs.
    select
        try_parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string as node_id,
        mode(warehouse_name) as build_warehouse_name
    from {{ ref('int_snowflake__query_history') }}
    where query_text like '%node_id%'
        and query_start_time >= dateadd(day, -30, current_timestamp())
        and warehouse_name is not null
    group by 1
),

-- Spillage measurements -------------------------------------------------------------
-- Each warehouse's spilling queries in the window: runtime for the aggregate signal's
-- cost and its scale-up effect.
warehouse_spill_runtime as (
    select
        qh.warehouse_name,
        sum(coalesce(qh.execution_time_ms, 0)) / 1000.0 as spilling_execution_s,
        max(wc.current_size) as warehouse_current_size
    from {{ ref('int_snowflake__query_history') }} as qh
    left join {{ ref('int_snowflake__warehouse_config') }} as wc
        on wc.warehouse_name = qh.warehouse_name
    where cast(qh.query_start_time as date) >= dateadd(day, -{{ spillage_lookback_days }}, current_date())
      and (qh.bytes_spilled_local > 0 or qh.bytes_spilled_remote > 0)
    group by qh.warehouse_name
),

-- Time each table's spilling operators spent blocked on disk (int_snowflake__query_spill_evidence,
-- from operator stats on a sample of its spilling queries), scaled from the sample to all
-- of its spilling queries: blocked_s x total runtime / sampled runtime.
table_spill_evidence as (
    select
        sp.table_fqn,
        count(*)                                    as sampled_query_count,
        sum(ev.execution_time_s)                    as sampled_execution_s,
        sum(ev.spill_blocked_s)                     as sampled_blocked_s,
        sum(ev.spill_blocked_s) * max(sp.spilling_execution_s)
            / nullif(sum(ev.execution_time_s), 0)   as spill_blocked_s_total
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    inner join {{ ref('int_snowflake__query_spill_evidence') }} as ev
        on ev.table_fqn = sp.table_fqn
       and ev.evidence_status = 'ok'
       and cast(ev.query_start_time as date) >= dateadd(day, -{{ spillage_lookback_days }}, current_date())
    group by sp.table_fqn
),

-- Hours saved and cost change for spillage recommendations, joined on in enriched.
spill_effects as (
    -- Scale-ups of one table's warehouse: from the performance model
    select
        'spillage_scale_up' as signal_id,
        sp.table_fqn as entity_name,
        sp.estimated_annual_hours_saved,
        sp.estimated_annual_cost_change_usd
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    where sp.recommendation_key in ('remote_spill', 'local_heavy_small_wh')

    union all

    -- SQL refactor: removing the spill saves at least the blocked time
    select
        'spillage_sql_refactor',
        tse.table_fqn,
        round(tse.spill_blocked_s_total / 3600.0 * {{ annualize_spillage }}, 2),
        null
    from table_spill_evidence as tse

    union all

    -- Aggregate scale-up of a warehouse: all its spilling queries
    select
        'spillage_scale_up',
        wsr.warehouse_name,
        round(wsr.spilling_execution_s
              * (1 - 1 / (2 * {{ warehouse_scale_up_efficiency('wsr.warehouse_current_size') }}))
              / 3600.0 * {{ annualize_spillage }}, 2),
        round(wsr.spilling_execution_s
              * coalesce({{ warehouse_credits_per_hour('wsr.warehouse_current_size') }}, 1) / 3600.0
              * (1 / {{ warehouse_scale_up_efficiency('wsr.warehouse_current_size') }} - 1)
              * {{ annualize_spillage }} * {{ credit_rate_usd }}, 2)
    from warehouse_spill_runtime as wsr
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
            -- 1.1: Auto-suspend reduction — save (current - 60)s per suspend cycle
            when ws.recommendation_key = 'idle_reduce_auto_suspend'
                then coalesce(sc.autosuspend_cycles_30d, 0)
                     * greatest(ws.auto_suspend_seconds - 60, 0) / 3600.0
                     * coalesce(wlr.credits_per_hour, 1)
                     * 12 * {{ credit_rate_usd }}
            -- 1.2: ECONOMY→STANDARD — eliminate ~150s idle per MCW spindown cycle
            when ws.recommendation_key = 'idle_switch_scaling_policy'
                then coalesce(sc.mcw_spindown_cycles_30d, 0)
                     * 150.0 / 3600.0
                     * coalesce(wlr.credits_per_hour, 1)
                     * 12 * {{ credit_rate_usd }}
            -- 1.3: Reduce max clusters — fewer spindown idle periods
            when ws.recommendation_key = 'idle_reduce_max_clusters'
                then coalesce(sc.mcw_spindown_cycles_30d, 0)
                     * 150.0 / 3600.0
                     * coalesce(wlr.credits_per_hour, 1)
                     * 12 * {{ credit_rate_usd }}
            -- 1.4: Reduce min clusters — eliminate forced-idle cluster time
            when ws.recommendation_key = 'idle_reduce_min_clusters'
                then (ws.min_cluster_count - 1)
                     * coalesce(wlr.credits_per_hour, 1)
                     * ws.avg_idle_credit_pct_30d * 720.0
                     * 12 * {{ credit_rate_usd }}
            -- 1.7: Consolidate underloaded — warehouse retires entirely
            when ws.recommendation_key = 'idle_consolidate_underloaded'
                then ws.total_idle_credits_30d * 12 * {{ credit_rate_usd }}
            -- 1.5: Consolidate standard — conservative 50%
            when ws.recommendation_key = 'idle_consolidate_standard'
                then ws.total_idle_credits_30d * 0.5 * 12 * {{ credit_rate_usd }}
            -- 1.6: Enable MCW bursty — adds cost, benefit is reduced queuing
            when ws.recommendation_key = 'idle_enable_mcw_bursty'
                then null
            -- Oversized: save ~50% by scaling down
            when ws.symptom = 'oversized'
                then ws.total_credits_30d * 0.50 * 12 * {{ credit_rate_usd }}
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
    left join {{ ref('int_snowflake__warehouse_suspend_cycles') }} as sc
        on sc.warehouse_name = ws.warehouse_name
    left join warehouse_list_rates as wlr on wlr.warehouse_name = ws.warehouse_name
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
        sp.recommendation_reason
            || case
                when sp.recommendation_key = 'local_heavy_large_wh' and tse.table_fqn is not null
                    then ' Measured on ' || tse.sampled_query_count || ' sampled spilling quer'
                        || iff(tse.sampled_query_count = 1, 'y', 'ies')
                        || ': spilling operators were blocked on disk for '
                        || to_varchar(round(100 * tse.sampled_blocked_s / nullif(tse.sampled_execution_s, 0)))
                        || '% of the runtime, the least a fix that removes the spill saves.'
                else ''
               end as recommendation_reason,
        -- Signal, effort and status come from the performance model's tier key, not the
        -- recommendation text (text matching mislabeled three tiers).
        case sp.recommendation_key
            when 'local_heavy_large_wh' then 'sql_refactor'
            when 'local_moderate_worsening' then 'investigation'
            when 'local_moderate_stable' then 'investigation'
            else 'config_change'
        end as effort_category,
        sp.total_gb_spilled_local + sp.total_gb_spilled_remote as score,
        -- Cost: the measured runtime of the table's spilling queries at its warehouse's
        -- list rate, annualized. Savings: for a SQL refactor, the time its spilling
        -- operators were blocked on disk (operator stats), priced the same way; null
        -- without operator stats. Scale-ups trade credits for time, so their savings are
        -- null and their effect is in estimated_annual_hours_saved and
        -- estimated_annual_cost_change_usd.
        sp.spilling_execution_s
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_spillage }} * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        case when sp.recommendation_key = 'local_heavy_large_wh' then
            tse.spill_blocked_s_total
                * coalesce(wlr.credits_per_hour, 1) / 3600.0
                * {{ annualize_spillage }} * {{ credit_rate_usd }}
        end::float as estimated_annual_savings_usd,
        sp.snowflake_ddl,
        sp.snapshot_date,
        case sp.recommendation_key
            when 'local_moderate_worsening' then 'monitor'
            when 'local_moderate_stable' then 'monitor'
            when 'local_minor' then 'stable'
            else 'actionable'
        end as backlog_status,
        null as dbt_model_config,
        null as identified_unique_key,
        case sp.recommendation_key
            when 'remote_spill' then 'spillage_scale_up'
            when 'local_heavy_small_wh' then 'spillage_scale_up'
            when 'local_heavy_large_wh' then 'spillage_sql_refactor'
            when 'local_moderate_worsening' then 'spillage_moderate_worsening'
            else 'spillage_moderate_stable'
        end as signal_id
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }} as sp
    left join warehouse_list_rates as wlr on wlr.warehouse_name = sp.warehouse_name
    left join table_spill_evidence as tse on tse.table_fqn = sp.table_fqn
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
            || 'the warehouse is undersized for the combined workload.'
            || coalesce(' Scaling up: about '
                || to_varchar(round(2 * {{ warehouse_scale_up_efficiency('sp_agg.warehouse_current_size') }}, 1))
                || 'x faster on its spilling queries, '
                || iff({{ warehouse_scale_up_efficiency('sp_agg.warehouse_current_size') }} < 1,
                       'about +' || to_varchar(round((1 / {{ warehouse_scale_up_efficiency('sp_agg.warehouse_current_size') }} - 1) * 100)) || '% credits',
                       'about the same credits')
                || '. Scaling up reduces spill; eliminating it may also need the models'' SQL or materialization changed.', '')
            as recommendation_reason,
        'config_change' as effort_category,
        sp_agg.total_gb_spilled as score,
        -- Cost: the runtime of all the warehouse's spilling queries in the window, at its
        -- list rate, annualized. Savings aren't estimated (null), as for per-table spillage.
        coalesce(wsq.spilling_execution_s, 0)
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_spillage }} * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        null::float as estimated_annual_savings_usd,
        'ALTER WAREHOUSE ' || sp_agg.warehouse_name || ' SET WAREHOUSE_SIZE = '''
            || {{ next_warehouse_size('sp_agg.warehouse_current_size', 'up') }} || ''';' as snowflake_ddl,
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
    left join warehouse_list_rates as wlr on wlr.warehouse_name = sp_agg.warehouse_name
    left join warehouse_spill_runtime as wsq on wsq.warehouse_name = sp_agg.warehouse_name

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
        -- The view's query runs on every read and on every build of a table downstream of
        -- it. Materialized, it runs once per dbt run (view_build_runs) instead. Each run
        -- costs recompute_cost_s: the view probe's measured time, else the average read.
        (tm.select_count + tm.downstream_build_count) * coalesce(tm.recompute_cost_s, 0)
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_materialization }} * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        tm.net_recompute_s_saved
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_materialization }} * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        tm.snapshot_date,
        case
            when tm.recommendation like '%Monitor%' then 'monitor'
            -- Another view in the same chain is recommended instead (see chosen_view_for_chain)
            when tm.chain_role = 'alternative' then 'monitor'
            else 'actionable'
        end as backlog_status,
        '{% raw %}{{ config(materialized=''table'') }}{% endraw %}' as dbt_model_config,
        null as identified_unique_key,
        'materialize_as_table' as signal_id
    from {{ ref('fct_snowflake__table_materialization_candidates') }} as tm
    left join model_warehouses as mw on mw.node_id = tm.dbt_model
    left join warehouse_list_rates as wlr on wlr.warehouse_name = mw.build_warehouse_name
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
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        ic.avg_build_time_sec * ic.builds_per_day * 365
            * coalesce(ic.rebuild_redundancy_rate, 0.5)
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
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
    left join model_warehouses as mw on mw.node_id = ic.dbt_model
    left join warehouse_list_rates as wlr on wlr.warehouse_name = mw.build_warehouse_name
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
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        icr.avg_build_time_sec * coalesce(icr.builds_per_day, 1) * 365
            * coalesce(icr.rebuild_redundancy_rate, 0.5)
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
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
    left join model_warehouses as mw on mw.node_id = icr.dbt_model
    left join warehouse_list_rates as wlr on wlr.warehouse_name = mw.build_warehouse_name
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
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_clustering }} * {{ credit_rate_usd }} as estimated_annual_cost_usd,
        tc.select_count * tc.avg_query_duration_s
            -- Filter share among the queries the operator-evidence hook analyzed (a sample of
            -- at most clustering_key_operator_queries_per_table per run), not among all reads.
            * (coalesce(ck_top.filter_query_count, 0)::float / nullif(ck_top.total_queries_analyzed, 0))
            * greatest(
                tc.scan_ratio_pct / 100.0
                - (1.0 / nullif(coalesce(ck_top.top_key_distinct_values, 1), 0)),
                0
            )
            * coalesce(wlr.credits_per_hour, 1) / 3600.0
            * {{ annualize_clustering }} * {{ credit_rate_usd }} as estimated_annual_savings_usd,
        null as snowflake_ddl,
        tc.snapshot_date,
        case when ck_top.table_fqn is not null then 'actionable' else 'monitor' end as backlog_status,
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
    left join model_warehouses as mw on mw.node_id = tc.dbt_model
    left join warehouse_list_rates as wlr on wlr.warehouse_name = mw.build_warehouse_name
    left join (
        select
            ck.table_fqn,
            ck.filter_query_count,
            ck.total_queries_analyzed,
            cc.distinct_values as top_key_distinct_values
        from {{ ref('fct_snowflake__clustering_key_candidates') }} as ck
        left join {{ ref('int_snowflake__column_cardinality') }} as cc
            on ck.table_fqn = cc.table_fqn and ck.column_name = cc.column_name
        where ck.recommended_key_position = 1
          and ck.snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__clustering_key_candidates') }})
    ) as ck_top on ck_top.table_fqn = tc.table_fqn
    where tc.is_candidate = true
        and tc.recommendation_status in ('evaluate_clustering', 'evaluate_key_alignment', 'insufficient_evidence')
        -- The fact table keeps one snapshot per run; only the latest is current.
        and tc.snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__table_clustering_candidates') }})

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
        se.estimated_annual_hours_saved,
        se.estimated_annual_cost_change_usd,
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
    -- Spillage only: hours saved and cost change (scale-ups), hours saved (SQL refactor)
    left join spill_effects as se
        on se.signal_id = ar.signal_id
       and se.entity_name = ar.entity_name
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
    -- Warehouse fallback: reuse model_warehouses CTE for the model's most common build warehouse
    left join model_warehouses as build_wh
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
                try_parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string,
                '.', 2
            ) in (
                {%- for proj in monitored_projects -%}
                    '{{ proj }}'{% if not loop.last %}, {% endif %}
                {%- endfor -%}
            )
    ) as wh_project
        on wh_project.warehouse_name = ar.warehouse_name
        and ar.dbt_model is null
    -- Leave out deployments excluded by dbt_excluded_targets / dbt_excluded_schemas
    -- (int_snowflake__dbt_relation_history.is_excluded), and tables in excluded schemas
    -- that relation history doesn't know. Warehouse-level rows have no table and stay.
    where not coalesce(rh.is_excluded, false)
      and not {{ relation_is_excluded("split_part(ar.table_fqn, '.', 2)", 'null') }}
),

-- =========================================================================
-- PRIORITY TIER LOGIC — Per-entity relative ordering
-- Priority is determined by hierarchy rank within each entity (model/warehouse).
-- A signal alone on an entity is P1. Co-occurring signals are ranked by hierarchy.
-- =========================================================================

ranked as (
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
                'spillage_scale_up', 'spillage_sql_refactor',
                'overload_enable_mcw', 'overload_scale_up_standard',
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
        end as hierarchy_rank
    from enriched as e
),

prioritized as (
    select
        r.*,
        -- Per-entity priority: rank 1 = do first for this entity.
        -- Actionable items always rank above monitor/stable regardless of hierarchy;
        -- then the hierarchy (defined once, above); then savings.
        row_number() over (
            partition by r.dedup_key
            order by
                case r.backlog_status
                    when 'actionable' then 1
                    when 'monitor' then 2
                    else 3
                end,
                r.hierarchy_rank,
                r.estimated_annual_savings_usd desc nulls last
        ) as priority_tier
    from ranked as r
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
    -- Spillage: scale-ups trade credits for time (savings null). Hours saved per year,
    -- and the change in annual cost (positive = costs more). SQL refactors: hours saved.
    estimated_annual_hours_saved,
    estimated_annual_cost_change_usd,
    snowflake_ddl,
    snapshot_date,
    case
        -- Demote only recommendations whose estimate exists and is below the floor.
        -- A null estimate means the benefit isn't expressed in dollars (e.g. MCW for
        -- bursty workloads), not that it's worthless, so those keep their status.
        when backlog_status = 'actionable'
             and estimated_annual_savings_usd is not null
             and estimated_annual_savings_usd < {{ min_savings }}
            then 'stable'
        else backlog_status
    end as backlog_status,
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
