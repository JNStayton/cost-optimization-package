{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='incremental_candidates_snapshot_key',
    post_hook="{{ probe_unique_key_candidates() }}"
) }}

{% set lookback_days = var('incremental_candidates_lookback_days', 60) %}
{% set min_avg_bytes_scanned_gb = var('incremental_candidates_min_avg_bytes_scanned_gb', 0.1) %}
{% set min_run_count = var('incremental_candidates_min_run_count', 3) %}
{% set large_table_gb_threshold = var('incremental_candidates_large_table_gb_threshold', 10) %}
{#-- minimum builds (with a produced_rows signal) required before rebuild_redundancy_rate is trusted.
     Mirrors Jessica's Snowflake min_qualified_build_days; lower it for very short lookback windows. --#}
{% set min_qualified_builds = var('incremental_candidates_min_qualified_builds', 3) %}

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
        max(start_time) as last_seen,
        -- growth signal for rebuild_redundancy_rate: produced_rows on each full
        -- rebuild approximates the table's row count at that build. The null key in
        -- min_by/max_by excludes runs with no produced_rows signal.
        count_if(produced_rows is not null) as qualified_build_count,
        min_by(produced_rows, case when produced_rows is not null then start_time end) as rows_at_period_start,
        max_by(produced_rows, case when produced_rows is not null then start_time end) as rows_at_period_end
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
    from {{ ref('int_databricks__table_inventory') }}
),

unique_key_candidates as (
    select
        catalog_name,
        schema_name,
        table_name,
        column_name,
        ordinal_position,
        case
            when lower(column_name) in ('surrogate_key', 'primary_key') then 10
            when lower(column_name) rlike '.+_id$'                       then 9
            when lower(column_name) rlike '.+_sk$'                       then 8
            when lower(column_name) = 'id'                               then 7
            when lower(column_name) in ('uuid', 'guid')                  then 6
            when lower(column_name) rlike '.+_(uuid|guid|key)$'         then 5
            else 0
        end as key_score
    from {{ ref('stg_databricks__columns') }}
    where data_type not in (
        'timestamp', 'timestamp_ntz', 'timestamp_ltz', 'date',
        'float', 'double', 'boolean'
    )
),

suggested_unique_keys as (
    with ranked as (
        select
            catalog_name,
            schema_name,
            table_name,
            column_name,
            row_number() over (
                partition by catalog_name, schema_name, table_name
                order by key_score desc, ordinal_position asc
            ) as key_rank
        from unique_key_candidates
        where key_score > 0
    )
    select
        catalog_name,
        schema_name,
        table_name,
        max(case when key_rank = 1 then column_name end) as suggested_unique_key,
        array_compact(
            array(
                max(case when key_rank = 1 then column_name end),
                max(case when key_rank = 2 then column_name end),
                max(case when key_rank = 3 then column_name end)
            )
        ) as unique_key_candidates
    from ranked
    where key_rank <= 3
    group by 1, 2, 3
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

graph_metadata as (
    select
        dbt_model,
        downstream_model_count
    from {{ ref('int_dbt__relations') }}
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
        -- rebuild redundancy: how much of each full rebuild re-processes rows that
        -- already existed at the start of the window (Jessica's first-build/last-build
        -- ratio). High rate = incrementalizing skips most of the scan.
        coalesce(mr.qualified_build_count, 0) as qualified_build_count,
        mr.rows_at_period_start,
        mr.rows_at_period_end,
        case
            when coalesce(mr.qualified_build_count, 0) >= {{ min_qualified_builds }}
                and mr.rows_at_period_end >= mr.rows_at_period_start
            then round(mr.rows_at_period_start / nullif(mr.rows_at_period_end, 0), 4)
        end as rebuild_redundancy_rate,
        -- false when history is too thin or row count shrank mid-window (likely a
        -- full-refresh or upstream deletes) — both make the ratio untrustworthy
        case
            when coalesce(mr.qualified_build_count, 0) < {{ min_qualified_builds }} then false
            when mr.rows_at_period_end < mr.rows_at_period_start then false
            else true
        end as growth_signal_reliable,
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
            when (coalesce(ds.update_count, 0) > 0 or coalesce(ds.merge_count, 0) > 0)
                and fcd.filter_column_data_type in ('timestamp', 'timestamp_ntz', 'timestamp_ltz', 'date')
                and mr.avg_bytes_scanned / power(1024, 3) >= {{ large_table_gb_threshold }}
                then 'delete+insert'
            when coalesce(ds.update_count, 0) > 0 or coalesce(ds.merge_count, 0) > 0
                then 'merge'
            when coalesce(ds.delete_count, 0) > 0
                then 'merge'
            when coalesce(ds.insert_count, 0) > 0
                and coalesce(ds.update_count, 0) = 0
                and coalesce(ds.delete_count, 0) = 0
                and coalesce(ds.merge_count,  0) = 0
                and fcd.filter_column_data_type in ('timestamp', 'timestamp_ntz', 'timestamp_ltz', 'date')
                and mr.avg_bytes_scanned / power(1024, 3) >= {{ large_table_gb_threshold }}
                then 'microbatch'
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
                and coalesce(tc.is_already_clustered, false) = false
                and fcd.filter_column_data_type in ('timestamp', 'timestamp_ntz', 'timestamp_ltz', 'date')
                then 'delete+insert'
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
        uk.unique_key_candidates,
        cast(null as string) as likely_unique_key,
        fc.suggested_filter_column,
        fc.suggested_filter_column_confidence,
        fcd.filter_column_data_type,
        coalesce(gm.downstream_model_count, 0) as downstream_model_count,
        (fc.suggested_filter_column is not null) as has_filter_column,
        (uk.suggested_unique_key is not null) as has_unique_key_candidate,
        (coalesce(ds.delete_count, 0) > 0) as has_external_deletes,
        (mr.avg_bytes_scanned / power(1024, 3) >= {{ large_table_gb_threshold }}) as is_large_table
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
    left join graph_metadata as gm
        on mr.node_id = gm.dbt_model
),

final_with_templates as (
    select
        *,
        -- ROI tier from the rebuild redundancy signal (null when unreliable)
        case
            when not growth_signal_reliable or rebuild_redundancy_rate is null then null
            when rebuild_redundancy_rate >= 0.9 then 'Strong Candidate'
            when rebuild_redundancy_rate >= 0.7 then 'Candidate'
            when rebuild_redundancy_rate >= 0.5 then 'Candidate — Moderate Redundancy'
            else 'Low ROI — Minimal Rebuild Redundancy'
        end as rebuild_redundancy_tier,
        -- projected monthly GB re-scanned on unchanged rows — the compute an
        -- incremental build would avoid. Null when the redundancy signal is unreliable.
        case
            when growth_signal_reliable and rebuild_redundancy_rate is not null
                then round(estimated_monthly_bytes_scanned_gb * rebuild_redundancy_rate, 2)
        end as estimated_monthly_redundant_gb_scanned,
        case
            when is_candidate
                then
                    'Avg ' || cast(avg_bytes_scanned_gb as string) || ' GB scanned per run across '
                    || cast(run_count as string) || ' runs in the last {{ lookback_days }} days'
                    || ' — incrementalization would reduce compute cost'
            else null
        end as recommendation_reason,
        case
            when suggested_incremental_strategy = 'delete+insert' and is_large_table
                and (update_count > 0 or merge_count > 0)
                then 'Mutable rows at large scale (' || cast(avg_bytes_scanned_gb as string)
                    || ' GB avg scan) — delete+insert scoped to ' || coalesce(suggested_filter_column, 'filter window')
                    || ' avoids the full-target scan that merge would do. Requires a reliable filter column to bound the delete window.'
            when suggested_incremental_strategy = 'merge' and suggested_incremental_strategy_confidence = 'HIGH' and has_external_deletes
                then 'UPDATE/MERGE and external DELETEs detected — merge applies updates via unique_key deduplication. WARNING: incremental builds will miss deletes that fall outside the load window. Schedule a periodic full-refresh, or add a reliable date/timestamp filter to enable delete+insert instead.'
            when suggested_incremental_strategy = 'merge' and suggested_incremental_strategy_confidence = 'HIGH'
                then 'UPDATE or MERGE statements detected — rows are mutable. merge strategy applies updates correctly via unique_key deduplication.'
            when suggested_incremental_strategy = 'merge' and suggested_incremental_strategy_confidence = 'MEDIUM'
                then 'External DELETEs detected without updates — merge handles deletions safely. WARNING: incremental builds will miss deletes that fall outside the load window. Schedule a periodic full-refresh, or use delete+insert with incremental_predicates if a reliable date/timestamp filter exists.'
            when suggested_incremental_strategy = 'merge' and suggested_incremental_strategy_confidence = 'LOW'
                then 'No DML history in the lookback window — merge is the safest default. Confirm with the source data owner before converting.'
            when suggested_incremental_strategy = 'microbatch'
                then 'Append-only pattern at large scale (' || cast(avg_bytes_scanned_gb as string)
                    || ' GB avg scan) with a date/timestamp filter — microbatch processes data in self-healing time batches (dbt Core 1.9+). Set begin to the earliest date you need to backfill and event_time to the filter column.'
            when suggested_incremental_strategy = 'insert_overwrite'
                then 'Insert-only pattern on a clustered table. insert_overwrite replaces affected partitions rather than doing row-level merge — faster for partition-aligned writes. Confirm the filter column aligns with the cluster key.'
            when suggested_incremental_strategy = 'delete+insert'
                then 'Insert-only pattern with a date/timestamp filter column but no cluster key. delete+insert drops the filter window and rewrites it — faster than merge for high-volume tables where a partition rewrite is cheaper than row-level deduplication. Requires a reliable filter column to bound the delete window.'
            when suggested_incremental_strategy = 'append'
                then 'Insert-only pattern with no cluster key or timestamp filter column. append adds new rows without deduplication — only use if the source is truly append-only and late-arriving data is not a concern.'
            else null
        end as strategy_notes,
        case
            when suggested_unique_key is not null
                then
                    'select count(*) = count(distinct ' || suggested_unique_key || ') as is_unique'
                    || ' from ' || table_fqn
            else null
        end as validate_uniqueness_sql,
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
            when suggested_incremental_strategy = 'delete+insert' and suggested_filter_column is not null
                then
                    '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || '    where ' || suggested_filter_column
                    || ' >= current_date() - INTERVAL 3 DAYS  -- adjust lookback window as needed' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            when suggested_incremental_strategy = 'append' and suggested_filter_column is not null
                then
                    '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || '    where ' || suggested_filter_column || ' > (select max('
                    || suggested_filter_column || ') from ' || '{' || '{' || ' this ' || '}' || '}' || ')' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            when suggested_incremental_strategy = 'microbatch'
                then
                    '-- microbatch handles filtering via the event_time config — no is_incremental() block needed.'
                    || ' dbt automatically scopes each batch to begin/batch_size in the config.'
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
            when suggested_incremental_strategy = 'delete+insert'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''delete+insert'',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
            when suggested_incremental_strategy = 'append'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''append'',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || ') ' || '}' || '}'
            when suggested_incremental_strategy = 'microbatch'
                then
                    '{' || '{' || ' config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''microbatch'',' || chr(10)
                    || '    event_time=''' || coalesce(suggested_filter_column, '<event_time_column>') || ''',' || chr(10)
                    || '    begin=''YYYY-MM-DD'',  -- TODO: set your historical start date' || chr(10)
                    || '    batch_size=''day'',' || chr(10)
                    || '    lookback=1' || chr(10)
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
