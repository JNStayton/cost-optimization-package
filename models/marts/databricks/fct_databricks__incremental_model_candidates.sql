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

table_dml_stats as (
    select
        table_database,
        table_schema,
        table_name,
        sum(insert_count) as insert_count,
        sum(update_count) as update_count,
        sum(delete_count) as delete_count,
        sum(merge_count)  as merge_count
    from {{ ref('int_databricks__table_query_stats_daily') }}
    where stats_date >= current_date() - INTERVAL {{ lookback_days }} DAYS
    group by 1, 2, 3
),

table_clustered as (
    select
        database_name,
        schema_name,
        table_name,
        is_already_clustered
    from {{ ref('int_table_inventory') }}
),

final as (
    select
        current_date() as snapshot_date,
        md5(
            cast(current_date() as string) || '|' || coalesce(mr.node_id, '')
        ) as incremental_candidates_snapshot_key,
        mr.node_id as dbt_model,
        mr.model_name,
        mr.materialized,
        mr.table_fqn,
        mr.database_name,
        mr.schema_name,
        mr.table_name,
        mr.run_count,
        round(mr.total_bytes_scanned / power(1024, 3), 4) as total_bytes_scanned_gb,
        round(mr.avg_bytes_scanned / power(1024, 3), 4) as avg_bytes_scanned_gb,
        round(mr.total_execution_time_ms / 1000.0, 2) as total_execution_time_s,
        round(mr.avg_execution_time_ms / 1000.0, 2) as avg_execution_time_s,
        round(mr.run_count / {{ lookback_days }}.0 * 30, 1) as estimated_monthly_runs,
        round((mr.avg_bytes_scanned / power(1024, 3)) * (mr.run_count / {{ lookback_days }}.0 * 30), 2) as estimated_monthly_bytes_scanned_gb,
        round((mr.avg_bytes_scanned / power(1024, 3)) * mr.run_count, 4) as score,
        case
            when mr.materialized = 'table'
                and mr.avg_bytes_scanned / power(1024, 3) >= {{ min_avg_bytes_scanned_gb }}
                and mr.run_count >= {{ min_run_count }}
            then true
            else false
        end as is_candidate,
        coalesce(ds.insert_count, 0) as insert_count,
        coalesce(ds.update_count, 0) as update_count,
        coalesce(ds.delete_count, 0) as delete_count,
        coalesce(ds.merge_count,  0) as merge_count,
        case
            when coalesce(ds.update_count, 0) > 0 or coalesce(ds.merge_count, 0) > 0
                then 'merge'
            when coalesce(ds.delete_count, 0) > 0
                then 'merge'
            when coalesce(ds.insert_count, 0) > 0
                and coalesce(ds.update_count, 0) = 0
                and coalesce(ds.delete_count, 0) = 0
                and coalesce(ds.merge_count,  0) = 0
                and coalesce(tc.is_already_clustered, false) = true
                then 'insert_overwrite'
            when coalesce(ds.insert_count, 0) > 0
                and coalesce(ds.update_count, 0) = 0
                and coalesce(ds.delete_count, 0) = 0
                and coalesce(ds.merge_count,  0) = 0
                then 'append'
            else 'merge'
        end as suggested_incremental_strategy,
        case
            when coalesce(ds.update_count, 0) > 0 or coalesce(ds.merge_count, 0) > 0
                then 'HIGH'
            when coalesce(ds.delete_count, 0) > 0
                then 'MEDIUM'
            when coalesce(ds.insert_count, 0) > 0
                and coalesce(ds.update_count, 0) = 0
                and coalesce(ds.delete_count, 0) = 0
                and coalesce(ds.merge_count,  0) = 0
                then 'LOW'
            else 'LOW'
        end as suggested_incremental_strategy_confidence,
        mr.first_seen,
        mr.last_seen
    from model_runs as mr
    left join table_dml_stats as ds
        on mr.database_name = ds.table_database
        and mr.schema_name = ds.table_schema
        and mr.table_name = ds.table_name
    left join table_clustered as tc
        on mr.database_name = tc.database_name
        and mr.schema_name = tc.schema_name
        and mr.table_name = tc.table_name
)

select * from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
