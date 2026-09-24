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

unique_key_candidates as (
    select
        catalog_name,
        schema_name,
        table_name,
        column_name,
        ordinal_position,
        case
            when lower(column_name) rlike '.+_id$'               then 9
            when lower(column_name) = 'id'                       then 8
            when lower(column_name) in ('uuid', 'guid')          then 7
            when lower(column_name) rlike '.+_(uuid|guid|key)$'  then 6
            else 0
        end as key_score
    from {{ ref('stg_databricks__columns') }}
),

suggested_unique_keys as (
    select
        catalog_name,
        schema_name,
        table_name,
        column_name as suggested_unique_key
    from (
        select
            *,
            row_number() over (
                partition by catalog_name, schema_name, table_name
                order by key_score desc, ordinal_position asc
            ) as key_rank
        from unique_key_candidates
        where key_score > 0
    )
    where key_rank = 1
),

filter_column_suggestions as (
    select
        catalog_name,
        schema_name,
        table_name,
        suggested_cluster_key            as suggested_filter_column,
        suggested_cluster_key_confidence as suggested_filter_column_confidence
    from {{ ref('int_databricks__column_cluster_suggestions') }}
),

filter_column_data_types as (
    select
        c.catalog_name,
        c.schema_name,
        c.table_name,
        c.data_type as filter_column_data_type
    from {{ ref('stg_databricks__columns') }} as c
    inner join filter_column_suggestions as fcs
        on lower(c.catalog_name) = lower(fcs.catalog_name)
        and lower(c.schema_name) = lower(fcs.schema_name)
        and lower(c.table_name) = lower(fcs.table_name)
        and lower(c.column_name) = lower(fcs.suggested_filter_column)
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
        mr.last_seen,
        uk.suggested_unique_key,
        fc.suggested_filter_column,
        fc.suggested_filter_column_confidence,
        fcd.filter_column_data_type
    from model_runs as mr
    left join table_dml_stats as ds
        on mr.database_name = ds.table_database
        and mr.schema_name = ds.table_schema
        and mr.table_name = ds.table_name
    left join table_clustered as tc
        on mr.database_name = tc.database_name
        and mr.schema_name = tc.schema_name
        and mr.table_name = tc.table_name
    left join suggested_unique_keys as uk
        on lower(mr.database_name) = lower(uk.catalog_name)
        and lower(mr.schema_name) = lower(uk.schema_name)
        and lower(mr.table_name) = lower(uk.table_name)
    left join filter_column_suggestions as fc
        on lower(mr.database_name) = lower(fc.catalog_name)
        and lower(mr.schema_name) = lower(fc.schema_name)
        and lower(mr.table_name) = lower(fc.table_name)
    left join filter_column_data_types as fcd
        on lower(mr.database_name) = lower(fcd.catalog_name)
        and lower(mr.schema_name) = lower(fcd.schema_name)
        and lower(mr.table_name) = lower(fcd.table_name)
),

final_with_templates as (
    select
        *,
        case
            when is_candidate
                then
                    'Avg ' || cast(avg_bytes_scanned_gb as string) || ' GB scanned per run across '
                    || cast(run_count as string) || ' runs in the last {{ lookback_days }} days'
                    || ' — incrementalization would reduce compute cost'
            else null
        end as recommendation_reason,
        case
            when suggested_incremental_strategy = 'merge' and suggested_filter_column is not null
                then
                    '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || '    where ' || suggested_filter_column || ' >= (' || chr(10)
                    || '        select max(' || suggested_filter_column || ') - INTERVAL 1 DAY'
                    || ' from ' || '{' || '{' || ' this ' || '}' || '}' || chr(10)
                    || '    )' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            when suggested_incremental_strategy = 'insert_overwrite' and suggested_filter_column is not null
                then
                    '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || '    where ' || suggested_filter_column
                    || ' >= current_date() - INTERVAL 3 DAYS  -- adjust lookback as needed' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            when suggested_incremental_strategy = 'append' and suggested_filter_column is not null
                then
                    '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || '    where ' || suggested_filter_column || ' > (select max('
                    || suggested_filter_column || ') from ' || '{' || '{' || ' this ' || '}' || '}' || ')' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            else
                '-- No suitable incremental filter column detected.'
                || ' Add an ' || '{' || '%' || ' if is_incremental() ' || '%' || '} filter manually.'
        end as incremental_filter_template,
        case
            when suggested_incremental_strategy = 'merge'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''merge'',' || chr(10)
                    || '    unique_key=''' || coalesce(suggested_unique_key, '-- TODO: add your surrogate key') || ''',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
            when suggested_incremental_strategy = 'insert_overwrite'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''insert_overwrite'',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
            when suggested_incremental_strategy = 'append'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''append'',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
            else
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''merge'',  -- verify strategy based on your data patterns' || chr(10)
                    || '    unique_key=''-- TODO: add your surrogate key'',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
        end as updated_model_config,
        case
            when suggested_incremental_strategy = 'merge'
                and filter_column_data_type in ('timestamp', 'timestamp_ntz', 'timestamp_ltz')
                and (insert_count > (update_count + merge_count) * 9 or (update_count + merge_count) = 0)
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''microbatch'',' || chr(10)
                    || '    event_time=''' || suggested_filter_column || ''',' || chr(10)
                    || '    begin=''YYYY-MM-DD'',  -- TODO: set your historical start date' || chr(10)
                    || '    batch_size=''day'',' || chr(10)
                    || '    lookback=1' || chr(10)
                    || ') ' || '}' || '}'
            else null
        end as microbatch_config_template
    from final
)

select * from final_with_templates
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
