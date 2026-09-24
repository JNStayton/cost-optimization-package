{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='model_run_summary_key',
    enabled=(target.type == 'databricks')
) }}

{% set lookback_days = var('model_run_summary_lookback_days', 7) %}
{% set half_lookback = (lookback_days / 2) | int %}

with model_runs as (
    select *
    from {{ ref('int_databricks__dbt_model_run_history') }}
    where start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),

early_runs as (
    select
        node_id,
        avg(execution_time_ms) as avg_execution_time_ms
    from model_runs
    where start_time < current_timestamp() - INTERVAL {{ half_lookback }} DAYS
    group by 1
),

recent_runs as (
    select
        node_id,
        avg(execution_time_ms) as avg_execution_time_ms
    from model_runs
    where start_time >= current_timestamp() - INTERVAL {{ half_lookback }} DAYS
    group by 1
),

aggregated as (
    select
        mr.node_id,
        mr.model_name,
        mr.materialized,
        mr.table_fqn,
        mr.database_name,
        mr.schema_name,
        mr.table_name,
        count(*)                                              as run_count,
        round(avg(mr.execution_time_ms) / 1000.0, 2)         as avg_execution_time_s,
        round(max(mr.execution_time_ms) / 1000.0, 2)         as max_execution_time_s,
        round(min(mr.execution_time_ms) / 1000.0, 2)         as min_execution_time_s,
        round(sum(mr.execution_time_ms) / 1000.0, 2)         as total_execution_time_s,
        round(avg(mr.bytes_scanned) / power(1024, 3), 4)     as avg_bytes_scanned_gb,
        round(max(mr.bytes_scanned) / power(1024, 3), 4)     as max_bytes_scanned_gb,
        round(sum(mr.bytes_scanned) / power(1024, 3), 4)     as total_bytes_scanned_gb,
        round(count(*) / {{ lookback_days }}.0 * 30, 1)      as estimated_monthly_runs,
        min(mr.start_time)                                    as first_run,
        max(mr.start_time)                                    as last_run
    from model_runs as mr
    group by 1, 2, 3, 4, 5, 6, 7
),

with_trend as (
    select
        a.*,
        round(
            avg_bytes_scanned_gb * estimated_monthly_runs,
            2
        ) as estimated_monthly_bytes_scanned_gb,
        case
            when er.avg_execution_time_ms is not null and er.avg_execution_time_ms > 0
            then round(
                (rr.avg_execution_time_ms - er.avg_execution_time_ms)
                    / er.avg_execution_time_ms * 100,
                1
            )
            else null
        end as execution_time_trend_pct
    from aggregated as a
    left join early_runs  as er using (node_id)
    left join recent_runs as rr using (node_id)
),

final as (
    select
        current_date() as snapshot_date,
        md5(
            cast(current_date() as string) || '|' || coalesce(node_id, '')
        ) as model_run_summary_key,
        node_id as dbt_model,
        model_name,
        materialized,
        table_fqn,
        database_name,
        schema_name,
        table_name,
        run_count,
        avg_execution_time_s,
        max_execution_time_s,
        min_execution_time_s,
        total_execution_time_s,
        avg_bytes_scanned_gb,
        max_bytes_scanned_gb,
        total_bytes_scanned_gb,
        estimated_monthly_runs,
        estimated_monthly_bytes_scanned_gb,
        execution_time_trend_pct,
        case
            when execution_time_trend_pct > 20  then 'DEGRADING'
            when execution_time_trend_pct < -20 then 'IMPROVING'
            when execution_time_trend_pct is null then 'INSUFFICIENT_DATA'
            else 'STABLE'
        end as performance_trend,
        first_run,
        last_run
    from with_trend
)

select * from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
