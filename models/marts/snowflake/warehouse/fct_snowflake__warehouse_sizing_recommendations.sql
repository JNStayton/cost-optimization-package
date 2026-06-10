{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Warehouse sizing recommendations for dbt workloads, based on concurrency,
  DML ratio, and idle credit signals over a 30-day lookback window.

  Mirrors the recommendation logic in find_warehouse_sizing_recommendations()
  but adds:
    - 7-day vs. 30-day trend comparison to detect drift (worsening / improving / stable)
    - Idle credit context from warehouse_metering_history
    - Persistent daily snapshot for historical tracking

  Recommendation tiers (same as the macro):
    Enable Gen2              — DML ratio > dml_threshold (default 35%)
    Enable multi-cluster     — median overload > 5s AND overload > 10% of execution
    Scale up single cluster  — median overload 1–5s (moderate concurrency bottleneck)
    Scale down single cluster — median execution < 5s AND median overload < 0.5s
    Stable                   — metrics within healthy range

  Trend direction compares the 7-day median overload to the 30-day median overload:
    Worsening  — 7-day overload > 30-day overload by more than 20%
    Improving  — 7-day overload < 30-day overload by more than 20%
    Stable     — within 20% of the 30-day baseline

  Controlled by the following dbt variables:
    - warehouse_sizing_lookback_days    (default 30)
    - warehouse_sizing_dml_threshold    (default 0.35)
    - warehouse_sizing_min_query_count  (default 20)
--#}

{% set lookback_days       = var('warehouse_sizing_lookback_days', 30) %}
{% set dml_threshold       = var('warehouse_sizing_dml_threshold', 0.35) %}
{% set min_query_count     = var('warehouse_sizing_min_query_count', 20) %}

with window_30d as (
    select
        warehouse_name,
        any_value(warehouse_size)                                   as warehouse_size,
        sum(total_dbt_queries)                                      as total_queries_30d,
        round(
            sum(dml_count) * 1.0 / nullif(sum(total_dbt_queries), 0),
            4
        )                                                           as dml_ratio_30d,
        round(median(median_overload_ms) / 1000.0, 2)              as median_overload_sec_30d,
        round(median(median_execution_ms) / 1000.0, 2)             as median_execution_sec_30d,
        round(median(overload_to_elapsed_ratio), 4)                 as overload_to_elapsed_ratio_30d,
        round(avg(avg_query_load_pct), 2)                           as avg_query_load_pct_30d,
        sum(queries_at_100pct_load)                                 as queries_at_100pct_load_30d
    from {{ ref('int_snowflake__warehouse_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by warehouse_name
    having sum(total_dbt_queries) >= {{ min_query_count }}
),

window_7d as (
    select
        warehouse_name,
        sum(total_dbt_queries)                                      as total_queries_7d,
        round(median(median_overload_ms) / 1000.0, 2)              as median_overload_sec_7d,
        round(median(median_execution_ms) / 1000.0, 2)             as median_execution_sec_7d
    from {{ ref('int_snowflake__warehouse_query_stats_daily') }}
    where stats_date >= dateadd(day, -7, current_date())
    group by warehouse_name
),

idle_30d as (
    select
        warehouse_name,
        round(avg(idle_credit_pct), 4)                              as avg_idle_credit_pct_30d,
        round(sum(idle_credits), 2)                                 as total_idle_credits_30d,
        round(sum(total_credits), 2)                                as total_credits_30d
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
        round(w30.dml_ratio_30d * 100, 1)                           as dml_pct_30d,
        w30.median_overload_sec_30d,
        w30.median_execution_sec_30d,
        w30.overload_to_elapsed_ratio_30d,
        w30.avg_query_load_pct_30d,
        w30.queries_at_100pct_load_30d,
        w7.median_overload_sec_7d,
        w7.median_execution_sec_7d,
        coalesce(id.avg_idle_credit_pct_30d, 0)                     as avg_idle_credit_pct_30d,
        coalesce(id.total_idle_credits_30d, 0)                      as total_idle_credits_30d,
        coalesce(id.total_credits_30d, 0)                           as total_credits_30d,
        case
            when w30.dml_ratio_30d > {{ dml_threshold }}
                then 'gen2'
            when w30.median_overload_sec_30d > 5
                 and w30.overload_to_elapsed_ratio_30d > 0.1
                then 'mcw'
            when w30.median_overload_sec_30d > 1
                 and w30.median_overload_sec_30d <= 5
                then 'scale_up'
            when w30.median_execution_sec_30d < 5
                 and w30.median_overload_sec_30d < 0.5
                then 'scale_down'
            else 'stable'
        end                                                         as recommendation_key,
        case
            when w7.median_overload_sec_7d > w30.median_overload_sec_30d * 1.2
                then 'Worsening'
            when w7.median_overload_sec_7d < w30.median_overload_sec_30d * 0.8
                then 'Improving'
            else 'Stable'
        end                                                         as overload_trend
    from window_30d as w30
    left join window_7d  as w7  on w7.warehouse_name  = w30.warehouse_name
    left join idle_30d   as id  on id.warehouse_name  = w30.warehouse_name
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
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
    median_overload_sec_7d,
    median_execution_sec_7d,
    overload_trend,
    avg_idle_credit_pct_30d,
    total_idle_credits_30d,
    total_credits_30d,
    case
        when recommendation_key = 'gen2'
            then 'Enable Gen2 warehouse (DML-heavy workload)'
        when recommendation_key = 'mcw'
            then 'Enable multi-cluster (Enterprise+) or split workload across warehouses'
        when recommendation_key = 'scale_up'
            then 'Scale up single cluster (moderate concurrency bottleneck)'
        when recommendation_key = 'scale_down'
            then 'Scale down single cluster (oversized for workload)'
        else
            'Stable — no sizing change recommended'
    end                                                             as recommendation,
    case
        when recommendation_key = 'gen2'
            then dml_pct_30d || '% of dbt queries are DML over the last '
                || {{ lookback_days }} || ' days (threshold: '
                || round({{ dml_threshold }} * 100, 0) || '%). '
                || 'Gen2 warehouses are optimized for DML-heavy workloads. '
                || 'Trend: ' || overload_trend || '.'
        when recommendation_key = 'mcw'
            then 'Median overload queue of ' || median_overload_sec_30d
                || 's over the last ' || {{ lookback_days }} || ' days exceeds 5s '
                || 'and is ' || round(overload_to_elapsed_ratio_30d * 100, 1)
                || '% of median elapsed time — high concurrency bottleneck. '
                || 'Trend: ' || overload_trend || '. '
                || 'Consider multi-cluster (Enterprise+) or splitting the workload '
                || 'across dedicated warehouses.'
        when recommendation_key = 'scale_up'
            then 'Median overload queue of ' || median_overload_sec_30d
                || 's over the last ' || {{ lookback_days }} || ' days indicates '
                || 'a moderate concurrency bottleneck. '
                || 'Trend: ' || overload_trend || '. '
                || 'Scaling up one size should reduce queue time.'
        when recommendation_key = 'scale_down'
            then 'Median execution of ' || median_execution_sec_30d
                || 's with negligible overload (' || median_overload_sec_30d
                || 's) over the last ' || {{ lookback_days }} || ' days — '
                || 'warehouse appears oversized for the workload. '
                || case
                    when avg_idle_credit_pct_30d > 0.1
                        then 'Idle credits are ' || round(avg_idle_credit_pct_30d * 100, 1)
                            || '% of total (' || total_idle_credits_30d
                            || ' idle credits in window) — scaling down will also '
                            || 'reduce idle spend.'
                    else 'Idle credit consumption is low.'
                   end
        else
            'Metrics within healthy range over the last ' || {{ lookback_days }} || ' days. '
            || 'Trend: ' || overload_trend || '. '
            || case
                when avg_idle_credit_pct_30d > 0.15
                    then 'Note: idle credits are ' || round(avg_idle_credit_pct_30d * 100, 1)
                        || '% of total (' || total_idle_credits_30d
                        || ' credits) — consider tightening auto-suspend settings.'
                else 'Consider SQL-level optimization or schedule tuning if performance is unexpected.'
               end
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'mcw'        then 1
        when 'gen2'       then 2
        when 'scale_up'   then 3
        when 'scale_down' then 4
        else 5
    end,
    total_queries_30d desc
