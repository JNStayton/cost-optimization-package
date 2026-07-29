{{
  config(
    materialized='table',
  )
}}

{#--
  Warehouse configuration recommendations based on the full symptom-to-optimization
  decision tree. Covers: idle credits, overload queuing, provisioning queue,
  oversized detection, and stable classification.

  Each recommendation includes:
    - Concrete DDL (ALTER WAREHOUSE) when applicable
    - Config-aware branching (auto_suspend, scaling_policy, MCW, edition)
    - Dynamic recommendation reasons with actual warehouse metrics

  Symptom priority (first match wins):
    1. High idle credits (idle_credit_pct > 0.20)
    2. Overload queuing (overload_to_elapsed_ratio > 0.10 OR median_overload_sec > 0.5)
    3. Provisioning queue (median_provisioning_ms > 2000)
    4. Oversized (low load + fast execution + no queuing)
    5. Stable / Monitor

  See docs/snowflake/warehouse_recommendations_mapping.md for full mapping.
--#}

{% set lookback_days    = var('warehouse_sizing_lookback_days', 30) %}
{% set min_query_count  = var('warehouse_sizing_min_query_count', 20) %}
{% set is_enterprise    = var('snowflake_enterprise_edition', true) %}

with window_30d as (
    select
        warehouse_name,
        max(warehouse_size) as warehouse_size,
        sum(total_dbt_queries) as total_queries_30d,
        round(sum(dml_count) * 1.0 / nullif(sum(total_dbt_queries), 0), 4) as dml_ratio_30d,
        round(median(median_overload_ms) / 1000.0, 2) as median_overload_sec_30d,
        round(median(median_execution_ms) / 1000.0, 2) as median_execution_sec_30d,
        round(median(overload_to_elapsed_ratio), 4) as overload_to_elapsed_ratio_30d,
        round(avg(avg_query_load_pct), 2) as avg_query_load_pct_30d,
        sum(queries_at_100pct_load) as queries_at_100pct_load_30d,
        round(median(median_provisioning_ms), 0) as median_provisioning_ms_30d
    from {{ ref('int_snowflake__warehouse_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by warehouse_name
    having sum(total_dbt_queries) >= {{ min_query_count }}
),

window_7d as (
    select
        warehouse_name,
        sum(total_dbt_queries) as total_queries_7d,
        round(median(median_overload_ms) / 1000.0, 2) as median_overload_sec_7d,
        round(median(median_execution_ms) / 1000.0, 2) as median_execution_sec_7d
    from {{ ref('int_snowflake__warehouse_query_stats_daily') }}
    where stats_date >= dateadd(day, -7, current_date())
    group by warehouse_name
),

idle_30d as (
    select
        warehouse_name,
        round(avg(idle_credit_pct), 4) as avg_idle_credit_pct_30d,
        round(sum(idle_credits), 2) as total_idle_credits_30d,
        round(sum(total_credits), 2) as total_credits_30d
    from {{ ref('int_snowflake__warehouse_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by warehouse_name
),

scored as (
    select
        w30.warehouse_name,
        w30.warehouse_size,
        w30.total_queries_30d,
        w7.total_queries_7d,
        w30.dml_ratio_30d,
        round(w30.dml_ratio_30d * 100, 1) as dml_pct_30d,
        w30.median_overload_sec_30d,
        w30.median_execution_sec_30d,
        w30.overload_to_elapsed_ratio_30d,
        w30.avg_query_load_pct_30d,
        w30.queries_at_100pct_load_30d,
        w30.median_provisioning_ms_30d,
        w7.median_overload_sec_7d,
        w7.median_execution_sec_7d,
        coalesce(id.avg_idle_credit_pct_30d, 0) as avg_idle_credit_pct_30d,
        coalesce(id.total_idle_credits_30d, 0) as total_idle_credits_30d,
        coalesce(id.total_credits_30d, 0) as total_credits_30d,
        -- Warehouse config
        coalesce(wc.is_gen2, false) as is_gen2,
        coalesce(wc.is_multicluster, false) as is_multicluster,
        coalesce(wc.is_adaptive, false) as is_adaptive,
        coalesce(wc.is_smallest_size, false) as is_smallest_size,
        coalesce(wc.current_warehouse_type, 'STANDARD') as current_warehouse_type,
        coalesce(wc.auto_suspend_seconds, 300) as auto_suspend_seconds,
        coalesce(wc.auto_resume, true) as auto_resume,
        coalesce(wc.scaling_policy, 'STANDARD') as scaling_policy,
        coalesce(wc.min_cluster_count, 1) as min_cluster_count,
        coalesce(wc.max_cluster_count, 1) as max_cluster_count,
        -- Trend
        case
            when w7.median_overload_sec_7d > w30.median_overload_sec_30d * 1.2 then 'Worsening'
            when w7.median_overload_sec_7d < w30.median_overload_sec_30d * 0.8 then 'Improving'
            else 'Stable'
        end as overload_trend
    from window_30d as w30
    left join window_7d as w7 on w7.warehouse_name = w30.warehouse_name
    left join idle_30d as id on id.warehouse_name = w30.warehouse_name
    left join {{ ref('int_snowflake__warehouse_config') }} as wc on w30.warehouse_name = wc.warehouse_name
),

classified as (
    select
        *,
        case
            -- Adaptive warehouses self-optimize
            when is_adaptive then 'stable'

            -- =====================================================================
            -- SYMPTOM 1: HIGH IDLE CREDITS (idle_credit_pct > 0.20)
            -- =====================================================================
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds > 60
                then 'idle_reduce_auto_suspend'                              -- 1.1
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and is_multicluster and scaling_policy = 'ECONOMY'
                 and {{ is_enterprise }}
                then 'idle_switch_scaling_policy'                            -- 1.2
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and is_multicluster and scaling_policy = 'STANDARD'
                 and max_cluster_count > 2
                 and {{ is_enterprise }}
                then 'idle_reduce_max_clusters'                              -- 1.3
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and is_multicluster and scaling_policy = 'STANDARD'
                 and max_cluster_count <= 2 and min_cluster_count > 1
                 and {{ is_enterprise }}
                then 'idle_reduce_min_clusters'                              -- 1.4
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and not is_multicluster and not {{ is_enterprise }}
                then 'idle_consolidate_standard'                             -- 1.5
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and not is_multicluster and {{ is_enterprise }}
                 and avg_query_load_pct_30d > 80
                then 'idle_enable_mcw_bursty'                                -- 1.6
            when avg_idle_credit_pct_30d > 0.20 and auto_suspend_seconds <= 60
                 and not is_multicluster and avg_query_load_pct_30d < 50
                then 'idle_consolidate_underloaded'                          -- 1.7

            -- =====================================================================
            -- SYMPTOM 2: OVERLOAD QUEUING (overload_ratio > 0.10 OR overload > 0.5s)
            -- =====================================================================
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and not is_multicluster and {{ is_enterprise }}
                then 'overload_enable_mcw'                                   -- 2.1
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and not is_multicluster and not {{ is_enterprise }}
                 and not is_smallest_size
                then 'overload_scale_up_standard'                            -- 2.2
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and not is_multicluster and not {{ is_enterprise }}
                 and is_smallest_size
                then 'overload_at_max_standard'                              -- 2.3 (smallest = can't scale, needs split)
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and is_multicluster and max_cluster_count < 10
                 and scaling_policy = 'ECONOMY'
                then 'overload_switch_scaling_policy'                        -- 2.4
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and is_multicluster and max_cluster_count < 10
                 and scaling_policy = 'STANDARD'
                then 'overload_increase_clusters'                            -- 2.5
            when (overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5)
                 and is_multicluster and max_cluster_count >= 10
                then 'overload_scale_up_large_mcw'                           -- 2.6

            -- =====================================================================
            -- SYMPTOM 3: PROVISIONING QUEUE (median_provisioning_ms > 2000)
            -- =====================================================================
            when median_provisioning_ms_30d > 2000 and auto_suspend_seconds < 60
                then 'provisioning_increase_suspend'                         -- 3.1
            when median_provisioning_ms_30d > 2000
                 and auto_suspend_seconds >= 60 and auto_suspend_seconds < 300
                then 'provisioning_increase_suspend_300'                     -- 3.2
            when median_provisioning_ms_30d > 2000 and not auto_resume
                then 'provisioning_enable_auto_resume'                       -- 3.3
            when median_provisioning_ms_30d > 2000
                 and auto_suspend_seconds >= 300 and not is_gen2
                then 'provisioning_gen2'                                     -- 3.4
            when median_provisioning_ms_30d > 2000
                 and is_multicluster and min_cluster_count = 0
                then 'provisioning_warm_cluster'                             -- 3.5

            -- =====================================================================
            -- SYMPTOM 5: OVERSIZED (low load + fast execution + no queuing)
            -- =====================================================================
            when avg_query_load_pct_30d < 50
                 and median_execution_sec_30d < 0.5
                 and overload_to_elapsed_ratio_30d < 0.01
                 and is_multicluster and max_cluster_count > 1
                 and avg_query_load_pct_30d < 30
                then 'oversized_disable_mcw'                                 -- 5.1
            when avg_query_load_pct_30d < 50
                 and median_execution_sec_30d < 0.5
                 and overload_to_elapsed_ratio_30d < 0.01
                 and not is_multicluster and not is_smallest_size
                 and median_execution_sec_30d < 0.2
                then 'oversized_scale_down'                                  -- 5.2
            when avg_query_load_pct_30d < 50
                 and median_execution_sec_30d < 0.5
                 and overload_to_elapsed_ratio_30d < 0.01
                 and is_smallest_size
                then 'oversized_at_minimum'                                  -- 5.3
            when avg_query_load_pct_30d < 50
                 and median_execution_sec_30d < 0.5
                 and overload_to_elapsed_ratio_30d < 0.01
                 and dml_ratio_30d > 0.20
                then 'oversized_write_heavy'                                 -- 5.4

            -- =====================================================================
            -- SYMPTOM 7: STABLE / MONITOR
            -- =====================================================================
            else 'stable'
        end as recommendation_key,
        -- Symptom classification
        case
            when avg_idle_credit_pct_30d > 0.20 then 'idle_credit_consumption'
            when overload_to_elapsed_ratio_30d > 0.10 or median_overload_sec_30d > 0.5 then 'query_overload'
            when median_provisioning_ms_30d > 2000 then 'queued_provisioning'
            when avg_query_load_pct_30d < 50 and median_execution_sec_30d < 0.5
                 and overload_to_elapsed_ratio_30d < 0.01 then 'oversized'
            else 'healthy'
        end as symptom
    from scored
)

select
    current_date() as snapshot_date,
    current_timestamp() as analyzed_at,
    {{ lookback_days }} as analysis_lookback_days,
    warehouse_name,
    warehouse_size,
    total_queries_30d,
    total_queries_7d,
    dml_pct_30d,
    median_overload_sec_30d,
    median_execution_sec_30d,
    overload_to_elapsed_ratio_30d,
    avg_query_load_pct_30d,
    queries_at_100pct_load_30d,
    median_provisioning_ms_30d,
    median_overload_sec_7d,
    median_execution_sec_7d,
    overload_trend,
    avg_idle_credit_pct_30d,
    total_idle_credits_30d,
    total_credits_30d,
    -- Config context
    is_gen2,
    is_multicluster,
    is_adaptive,
    is_smallest_size,
    current_warehouse_type,
    auto_suspend_seconds,
    auto_resume,
    scaling_policy,
    min_cluster_count,
    max_cluster_count,
    -- Classification
    recommendation_key,
    symptom,
    -- Recommendation text
    case
        when recommendation_key = 'idle_reduce_auto_suspend'
            then 'Reduce auto-suspend to 60 seconds'
        when recommendation_key = 'idle_switch_scaling_policy'
            then 'Switch scaling policy from ECONOMY to STANDARD'
        when recommendation_key = 'idle_reduce_max_clusters'
            then 'Reduce max cluster count'
        when recommendation_key = 'idle_reduce_min_clusters'
            then 'Set min cluster count to 1'
        when recommendation_key = 'idle_consolidate_standard'
            then 'Consolidate workloads (Standard edition)'
        when recommendation_key = 'idle_enable_mcw_bursty'
            then 'Enable multi-cluster for bursty workload'
        when recommendation_key = 'idle_consolidate_underloaded'
            then 'Consolidate underloaded warehouse'
        when recommendation_key = 'overload_enable_mcw'
            then 'Enable multi-cluster (queries queuing)'
        when recommendation_key = 'overload_scale_up_standard'
            then 'Scale up warehouse (Standard edition, queries queuing)'
        when recommendation_key = 'overload_at_max_standard'
            then 'Split workloads across warehouses (at capacity)'
        when recommendation_key = 'overload_switch_scaling_policy'
            then 'Switch scaling policy from ECONOMY to STANDARD (queries queuing despite MCW)'
        when recommendation_key = 'overload_increase_clusters'
            then 'Increase max cluster count'
        when recommendation_key = 'overload_scale_up_large_mcw'
            then 'Scale up warehouse size (MCW at cluster ceiling)'
        when recommendation_key = 'provisioning_increase_suspend'
            then 'Increase auto-suspend to 60 seconds (reduce cold starts)'
        when recommendation_key = 'provisioning_increase_suspend_300'
            then 'Increase auto-suspend to 300 seconds (frequent cold starts)'
        when recommendation_key = 'provisioning_enable_auto_resume'
            then 'Enable auto-resume'
        when recommendation_key = 'provisioning_gen2'
            then 'Migrate to Gen2 (faster provisioning)'
        when recommendation_key = 'provisioning_warm_cluster'
            then 'Set min cluster count to 1 (keep one cluster warm)'
        when recommendation_key = 'oversized_disable_mcw'
            then 'Disable multi-cluster (oversized for workload)'
        when recommendation_key = 'oversized_scale_down'
            then 'Scale down warehouse'
        when recommendation_key = 'oversized_at_minimum'
            then 'Stable — already at minimum size'
        when recommendation_key = 'oversized_write_heavy'
            then 'Monitor — do not scale down (write-heavy)'
        else 'Stable — no configuration change recommended'
    end as recommendation,
    -- Recommendation reason (verbose, with dynamic values)
    case
        when recommendation_key = 'idle_reduce_auto_suspend'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1) || '% of the time. '
                || 'Current auto_suspend is ' || auto_suspend_seconds || 's — reducing to 60s will eliminate ~'
                || round(total_idle_credits_30d * 0.7, 0) || ' idle credits/month without impacting query performance for most workloads.'
        when recommendation_key = 'idle_switch_scaling_policy'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1) || '% despite auto_suspend='
                || auto_suspend_seconds || 's. ECONOMY scaling policy keeps clusters running for 2-3 extra minutes after load drops. '
                || 'STANDARD scaling shuts down idle clusters immediately.'
        when recommendation_key = 'idle_reduce_max_clusters'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1)
                || '% with aggressive suspend and STANDARD scaling. Reducing max clusters from '
                || max_cluster_count || ' to ' || (max_cluster_count - 1)
                || ' limits over-provisioning while still allowing scale-out.'
        when recommendation_key = 'idle_reduce_min_clusters'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1)
                || '% but min_cluster_count=' || min_cluster_count
                || ' forces clusters to stay running. Setting min to 1 allows full scale-down during low-demand periods.'
        when recommendation_key = 'idle_consolidate_standard'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1)
                || '% with optimal suspend settings. Consolidate workloads onto fewer warehouses or schedule batch jobs '
                || 'into tighter windows to reduce total active time. MCW is not available on Standard edition.'
        when recommendation_key = 'idle_enable_mcw_bursty'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1)
                || '% but also hitting high load (' || avg_query_load_pct_30d
                || '%). This pattern suggests bursty workloads — MCW with STANDARD scaling will handle bursts and scale to zero between them.'
        when recommendation_key = 'idle_consolidate_underloaded'
            then 'Warehouse is idle ' || round(avg_idle_credit_pct_30d * 100, 1)
                || '% and underloaded when active (' || avg_query_load_pct_30d
                || '% avg load). Consider consolidating this warehouse''s workloads into another warehouse to eliminate the idle overhead entirely.'
        when recommendation_key = 'overload_enable_mcw'
            then 'Queries are spending ' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% of elapsed time queued behind other queries. '
                || 'Enabling multi-cluster allows concurrent workloads to scale out rather than queue.'
        when recommendation_key = 'overload_scale_up_standard'
            then 'Queries are spending ' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% of elapsed time queued. MCW is not available on Standard edition. '
                || 'Scaling up from ' || warehouse_size || ' doubles compute capacity to reduce concurrency pressure.'
        when recommendation_key = 'overload_at_max_standard'
            then 'Queries are spending ' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% of elapsed time queued. Warehouse is already at maximum size and MCW is unavailable on Standard edition. '
                || 'Options: upgrade edition, split workloads across multiple warehouses, or stagger scheduled jobs.'
        when recommendation_key = 'overload_switch_scaling_policy'
            then 'Queries are queuing (' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% overload ratio) despite MCW being enabled. ECONOMY scaling waits 2-3 minutes before adding clusters — '
                || 'switching to STANDARD will spin up new clusters immediately when load increases.'
        when recommendation_key = 'overload_increase_clusters'
            then 'Queries are queuing (' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% overload ratio) with STANDARD scaling at max_cluster_count=' || max_cluster_count
                || '. The warehouse is hitting its cluster ceiling — increasing by 1 provides additional burst capacity.'
        when recommendation_key = 'overload_scale_up_large_mcw'
            then 'Queries are queuing (' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% overload ratio) with MCW at ' || max_cluster_count
                || ' clusters. Cluster count is already high — scaling up warehouse size will give each cluster more compute capacity.'
        when recommendation_key = 'provisioning_increase_suspend'
            then 'Median provisioning wait is ' || median_provisioning_ms_30d
                || 'ms — queries are waiting for the warehouse to cold-start. Current auto_suspend='
                || auto_suspend_seconds || 's is aggressive, causing frequent suspend/resume cycles. '
                || 'Increasing to 60s reduces cold starts while keeping idle costs minimal.'
        when recommendation_key = 'provisioning_increase_suspend_300'
            then 'Median provisioning wait is ' || median_provisioning_ms_30d
                || 'ms despite auto_suspend=' || auto_suspend_seconds
                || 's. Workload pattern has frequent gaps > ' || auto_suspend_seconds
                || 's but < 5min. Increasing suspend to 300s will keep the warehouse warm between bursts.'
        when recommendation_key = 'provisioning_enable_auto_resume'
            then 'Median provisioning wait is ' || median_provisioning_ms_30d
                || 'ms. AUTO_RESUME is disabled — queries cannot start until the warehouse is manually resumed. '
                || 'Enable AUTO_RESUME to eliminate manual bottleneck.'
        when recommendation_key = 'provisioning_gen2'
            then 'Median provisioning wait is ' || median_provisioning_ms_30d
                || 'ms despite generous auto_suspend (' || auto_suspend_seconds
                || 's). Gen2 warehouses provision faster due to improved infrastructure. Consider migrating to Gen2.'
        when recommendation_key = 'provisioning_warm_cluster'
            then 'Median provisioning wait is ' || median_provisioning_ms_30d
                || 'ms. MCW is configured with min_cluster_count=0, meaning all clusters can fully suspend. '
                || 'Setting min=1 keeps one cluster warm to serve initial queries instantly.'
        when recommendation_key = 'oversized_disable_mcw'
            then 'Warehouse is oversized — running at ' || avg_query_load_pct_30d
                || '% avg load with negligible queuing. Multi-cluster is unnecessary at this concurrency level. '
                || 'Switching to single-cluster saves the MCW overhead and simplifies cost attribution.'
        when recommendation_key = 'oversized_scale_down'
            then 'Warehouse is oversized — median query execution is ' || round(median_execution_sec_30d * 1000, 0)
                || 'ms with only ' || avg_query_load_pct_30d || '% load. Scaling down from '
                || warehouse_size || ' halves credit consumption with minimal performance impact.'
        when recommendation_key = 'oversized_at_minimum'
            then 'Warehouse is at minimum size (X-Small). Load is low (' || avg_query_load_pct_30d
                || '%) — consider consolidating this warehouse''s workloads into another warehouse if feasible.'
        when recommendation_key = 'oversized_write_heavy'
            then 'Warehouse appears oversized by query metrics but has significant DML load (' || dml_pct_30d
                || '%). Write-heavy workloads benefit from larger warehouse sizes for COPY/INSERT performance even when concurrency metrics are low.'
        else
            'Metrics within healthy range over the last ' || {{ lookback_days }} || ' days. '
            || 'Trend: ' || overload_trend || '. '
            || case
                when avg_idle_credit_pct_30d > 0.15
                    then 'Note: idle credits are ' || round(avg_idle_credit_pct_30d * 100, 1)
                        || '% of total (' || total_idle_credits_30d || ' credits) — approaching action threshold.'
                else 'No configuration changes needed at this time.'
               end
    end as recommendation_reason,
    -- Concrete DDL
    case
        when recommendation_key = 'idle_reduce_auto_suspend'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET AUTO_SUSPEND = 60;'
        when recommendation_key = 'idle_switch_scaling_policy'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET SCALING_POLICY = ''STANDARD'';'
        when recommendation_key = 'idle_reduce_max_clusters'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MAX_CLUSTER_COUNT = ' || (max_cluster_count - 1) || ';'
        when recommendation_key = 'idle_reduce_min_clusters'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MIN_CLUSTER_COUNT = 1;'
        when recommendation_key = 'idle_enable_mcw_bursty'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = ''STANDARD'';'
        when recommendation_key = 'overload_enable_mcw'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = ''STANDARD'';'
        when recommendation_key = 'overload_scale_up_standard'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET WAREHOUSE_SIZE = '''
                || case warehouse_size
                    when 'X-Small' then 'SMALL'
                    when 'Small' then 'MEDIUM'
                    when 'Medium' then 'LARGE'
                    when 'Large' then 'XLARGE'
                    when 'X-Large' then '2X-LARGE'
                    else 'MEDIUM'
                end || ''';'
        when recommendation_key = 'overload_switch_scaling_policy'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET SCALING_POLICY = ''STANDARD'';'
        when recommendation_key = 'overload_increase_clusters'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MAX_CLUSTER_COUNT = ' || (max_cluster_count + 1) || ';'
        when recommendation_key = 'overload_scale_up_large_mcw'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET WAREHOUSE_SIZE = '''
                || case warehouse_size
                    when 'X-Small' then 'SMALL'
                    when 'Small' then 'MEDIUM'
                    when 'Medium' then 'LARGE'
                    when 'Large' then 'XLARGE'
                    when 'X-Large' then '2X-LARGE'
                    else 'MEDIUM'
                end || ''';'
        when recommendation_key = 'provisioning_increase_suspend'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET AUTO_SUSPEND = 60;'
        when recommendation_key = 'provisioning_increase_suspend_300'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET AUTO_SUSPEND = 300;'
        when recommendation_key = 'provisioning_enable_auto_resume'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET AUTO_RESUME = TRUE;'
        when recommendation_key = 'provisioning_gen2'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET RESOURCE_CONSTRAINT = ''STANDARD_GEN_2'';'
        when recommendation_key = 'provisioning_warm_cluster'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MIN_CLUSTER_COUNT = 1;'
        when recommendation_key = 'oversized_disable_mcw'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET MAX_CLUSTER_COUNT = 1;'
        when recommendation_key = 'oversized_scale_down'
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET WAREHOUSE_SIZE = '''
                || case warehouse_size
                    when 'Small' then 'X-SMALL'
                    when 'Medium' then 'SMALL'
                    when 'Large' then 'MEDIUM'
                    when 'X-Large' then 'LARGE'
                    when '2X-Large' then 'X-LARGE'
                    else 'X-SMALL'
                end || ''';'
        else null
    end as snowflake_ddl
from classified
order by
    case symptom
        when 'idle_credit_consumption' then 1
        when 'query_overload' then 2
        when 'queued_provisioning' then 3
        when 'oversized' then 4
        else 5
    end,
    total_credits_30d desc
