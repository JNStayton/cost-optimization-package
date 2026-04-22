{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='incremental_candidates_snapshot_key',
    enabled=(target.type == 'databricks')
) }}

{% set lookback_days = var('incremental_candidates_lookback_days', 7) %}
{% set min_avg_bytes_scanned_gb = var('incremental_candidates_min_avg_bytes_scanned_gb', 0.1) %}
{% set min_run_count = var('incremental_candidates_min_run_count', 3) %}

with model_runs as (
    select
        node_id,
        model_name,
        materialized,
        table_fqn,
        database_name,
        schema_name,
        table_name,
        count(*) as run_count,
        sum(bytes_scanned) as total_bytes_scanned,
        avg(bytes_scanned) as avg_bytes_scanned,
        sum(execution_time_ms) as total_execution_time_ms,
        avg(execution_time_ms) as avg_execution_time_ms,
        min(start_time) as first_seen,
        max(start_time) as last_seen
    from {{ ref('int_databricks__dbt_model_run_history') }}
    where start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        current_date() as snapshot_date,
        md5(
            cast(current_date() as string) || '|' || coalesce(node_id, '')
        ) as incremental_candidates_snapshot_key,
        node_id as dbt_model,
        model_name,
        materialized,
        table_fqn,
        database_name,
        schema_name,
        table_name,
        run_count,
        round(total_bytes_scanned / power(1024, 3), 4) as total_bytes_scanned_gb,
        round(avg_bytes_scanned / power(1024, 3), 4) as avg_bytes_scanned_gb,
        round(total_execution_time_ms / 1000.0, 2) as total_execution_time_s,
        round(avg_execution_time_ms / 1000.0, 2) as avg_execution_time_s,
        round(run_count / {{ lookback_days }}.0 * 30, 1) as estimated_monthly_runs,
        round((avg_bytes_scanned / power(1024, 3)) * (run_count / {{ lookback_days }}.0 * 30), 2) as estimated_monthly_bytes_scanned_gb,
        round((avg_bytes_scanned / power(1024, 3)) * run_count, 4) as score,
        case
            when materialized = 'table'
                and avg_bytes_scanned / power(1024, 3) >= {{ min_avg_bytes_scanned_gb }}
                and run_count >= {{ min_run_count }}
            then true
            else false
        end as is_candidate,
        first_seen,
        last_seen
    from model_runs
)

select * from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
