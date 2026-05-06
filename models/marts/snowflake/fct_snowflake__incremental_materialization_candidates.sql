{{
  config(
    materialized='table',
    enabled=false
  )
}}

{#--
  dbt TABLE models that are candidates for conversion to INCREMENTAL materialization,
  based on slow full-rebuild times and/or large table size over a configurable
  lookback window.

  Flagged when EITHER condition is met (OR logic):
    - max full-rebuild time >= incremental_candidates_min_build_time_sec
    - table size >= incremental_candidates_min_size_gb

  Surfaces copy-pasteable starter code: incremental_filter_template (the
  is_incremental() WHERE block) and updated_model_config (the config() block).
  When microbatch is a strong alternative (high insert ratio + TIMESTAMP column),
  microbatch_config_template is also populated.

  Controlled by the following dbt variables:
    - incremental_candidates_min_build_time_sec (default 300)
    - incremental_candidates_min_size_gb        (default 10)
    - incremental_candidates_lookback_days      (default 30)
--#}

{% set min_build_time_sec = var('incremental_candidates_min_build_time_sec', 300) %}
{% set min_size_gb        = var('incremental_candidates_min_size_gb', 10) %}
{% set lookback_days      = var('incremental_candidates_lookback_days', 30) %}

with table_candidates as (
    select
        upper(database_name) as database_name,
        upper(schema_name)   as schema_name,
        upper(table_name)    as table_name,
        upper(database_name) || '.' || upper(schema_name) || '.' || upper(table_name) as table_fqn,
        dbt_model,
        model_name,
        package_name
    from {{ ref('int_dbt__relations') }}
    where lower(materialized) = 'table'
),

build_stats as (
    select
        upper(table_database) || '.' || upper(table_schema) || '.' || upper(table_name) as table_fqn,
        coalesce(sum(select_count), 0)                                                   as select_count,
        coalesce(sum(dml_count), 0)                                                      as dml_count,
        coalesce(sum(insert_count), 0)                                                   as insert_count,
        coalesce(sum(update_count), 0)                                                   as update_count,
        coalesce(sum(delete_count), 0)                                                   as delete_count,
        coalesce(sum(merge_count), 0)                                                    as merge_count,
        coalesce(sum(table_build_count), 0)                                              as table_build_count,
        max(max_build_time_ms)                                                           as max_build_time_ms,
        avg(avg_build_time_ms)                                                           as avg_build_time_ms,
        iff(
            coalesce(sum(select_count), 0) > 0,
            coalesce(sum(select_execution_time_ms_sum), 0) / nullif(sum(select_count), 0),
            0
        )                                                                                as avg_query_execution_time_ms
    from {{ ref('int_snowflake__table_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by 1
),

table_size as (
    select
        upper(database_name) || '.' || upper(schema_name) || '.' || upper(table_name) as table_fqn,
        round(active_bytes / power(1024, 3), 2) as size_gb
    from {{ ref('int_snowflake__table_storage') }}
),

-- detect best TIMESTAMP column per table (preferred incremental filter key)
timestamp_keys as (
    select
        table_fqn,
        column_name,
        data_type,
        row_number() over (
            partition by table_fqn
            order by
                case
                    when lower(column_name) like '%updated_at%'    then 1
                    when lower(column_name) like '%_at'            then 2
                    when lower(column_name) like '%created%'       then 3
                    when lower(column_name) like '%_date'          then 4
                    when lower(column_name) like 'date%'           then 5
                    else 6
                end,
                ordinal_position
        ) as rn
    from {{ ref('int_snowflake__table_columns') }}
    where data_type in ('TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ')
),

-- detect best DATE column per table (fallback incremental filter key)
date_keys as (
    select
        table_fqn,
        column_name,
        data_type,
        row_number() over (
            partition by table_fqn
            order by
                case
                    when lower(column_name) like '%_date'  then 1
                    when lower(column_name) like 'date%'   then 2
                    when lower(column_name) like '%dt'     then 3
                    else 4
                end,
                ordinal_position
        ) as rn
    from {{ ref('int_snowflake__table_columns') }}
    where data_type = 'DATE'
),

-- detect best numeric ID/key column per table (unique_key candidate for merge)
id_keys as (
    select
        table_fqn,
        column_name,
        row_number() over (
            partition by table_fqn
            order by
                case
                    when lower(column_name) like '%_key'   then 1
                    when upper(column_name) = 'ID'         then 2
                    when lower(column_name) like '%_id'    then 3
                    else 4
                end,
                ordinal_position
        ) as rn
    from {{ ref('int_snowflake__table_columns') }}
    where data_type in ('NUMBER', 'INTEGER', 'BIGINT', 'INT', 'SMALLINT', 'TINYINT')
        and (
            upper(column_name) = 'ID'
            or lower(column_name) like '%_id'
            or lower(column_name) like '%_key'
        )
),

best_keys as (
    select
        tc.table_fqn,
        tk.column_name as best_timestamp_col,
        tk.data_type   as timestamp_type,
        dk.column_name as best_date_col,
        ik.column_name as best_id_col
    from table_candidates as tc
    left join (select * from timestamp_keys where rn = 1) as tk on tc.table_fqn = tk.table_fqn
    left join (select * from date_keys      where rn = 1) as dk on tc.table_fqn = dk.table_fqn
    left join (select * from id_keys        where rn = 1) as ik on tc.table_fqn = ik.table_fqn
),

scored as (
    select
        tc.table_fqn,
        tc.database_name,
        tc.schema_name,
        tc.table_name,
        tc.dbt_model,
        tc.model_name,
        tc.package_name,
        coalesce(ts.size_gb, 0)                                    as size_gb,
        coalesce(bs.select_count, 0)                               as select_count,
        coalesce(bs.dml_count, 0)                                  as dml_count,
        coalesce(bs.insert_count, 0)                               as insert_count,
        coalesce(bs.update_count, 0)                               as update_count,
        coalesce(bs.delete_count, 0)                               as delete_count,
        coalesce(bs.merge_count, 0)                                as merge_count,
        coalesce(bs.table_build_count, 0)                          as table_build_count,
        round(coalesce(bs.max_build_time_ms, 0) / 1000.0, 1)      as max_build_time_sec,
        round(coalesce(bs.avg_build_time_ms, 0) / 1000.0, 1)      as avg_build_time_sec,
        round(coalesce(bs.avg_query_execution_time_ms, 0) / 1000.0, 2) as avg_query_duration_s,
        bk.best_timestamp_col,
        bk.timestamp_type,
        bk.best_date_col,
        bk.best_id_col,
        -- incremental filter key: TIMESTAMP preferred over DATE
        coalesce(bk.best_timestamp_col, bk.best_date_col)          as suggested_incremental_key,
        -- unique key for merge strategy: numeric id/key column
        bk.best_id_col                                             as suggested_unique_key,
        -- strategy: merge when updates exist or TIMESTAMP found; insert_overwrite for DATE-only;
        --           append when only a numeric ID and no date/timestamp
        case
            when bk.best_timestamp_col is not null
                then 'merge'
            when bk.best_date_col is not null and bk.best_timestamp_col is null
                then 'insert_overwrite'
            when bk.best_timestamp_col is null
                and bk.best_date_col is null
                and bk.best_id_col is not null
                then 'append'
            else null
        end as suggested_strategy,
        -- flag what triggered the recommendation
        case
            when coalesce(bs.max_build_time_ms, 0) / 1000.0 >= {{ min_build_time_sec }}
                and coalesce(ts.size_gb, 0) >= {{ min_size_gb }}
                then 'slow_build_and_large'
            when coalesce(bs.max_build_time_ms, 0) / 1000.0 >= {{ min_build_time_sec }}
                then 'slow_build'
            when coalesce(ts.size_gb, 0) >= {{ min_size_gb }}
                then 'large_table'
        end as trigger_reason
    from table_candidates as tc
    left join build_stats as bs on tc.table_fqn = bs.table_fqn
    left join table_size  as ts on tc.table_fqn = ts.table_fqn
    left join best_keys   as bk on tc.table_fqn = bk.table_fqn
    where
        coalesce(bs.max_build_time_ms, 0) / 1000.0 >= {{ min_build_time_sec }}
        or coalesce(ts.size_gb, 0) >= {{ min_size_gb }}
)

select
    current_date()      as snapshot_date,
    current_timestamp() as analyzed_at,
    -- identity
    table_fqn,
    database_name,
    schema_name,
    table_name,
    dbt_model,
    model_name,
    package_name,
    -- evidence: size and build performance
    size_gb                                                         as table_size_gb,
    table_build_count,
    max_build_time_sec,
    avg_build_time_sec,
    -- evidence: query activity
    select_count,
    avg_query_duration_s,
    -- evidence: dml breakdown (informs strategy recommendation)
    insert_count,
    update_count,
    delete_count,
    merge_count,
    -- recommendation
    coalesce(suggested_strategy, 'Manual Check')                    as suggested_strategy,
    coalesce(suggested_incremental_key, '-- no suitable key detected') as suggested_incremental_key,
    coalesce(suggested_unique_key, '-- TODO: add your surrogate key column') as suggested_unique_key,
    case
        when trigger_reason = 'slow_build_and_large'
            then 'Large table (' || table_size_gb || ' GB) with slow builds (max '
                || max_build_time_sec || 's) — incrementalization would reduce both rebuild time and compute cost'
        when trigger_reason = 'slow_build'
            then 'Slow table builds detected (max ' || max_build_time_sec
                || 's) — incrementalization would reduce rebuild time'
        when trigger_reason = 'large_table'
            then 'Large table (' || table_size_gb || ' GB) — incrementalization would reduce compute cost,'
                || ' especially at smaller warehouse sizes'
    end as recommendation_reason,
    -- copy-pasteable is_incremental() filter block
    case
        when coalesce(suggested_strategy, 'Manual Check') = 'merge'
            and suggested_incremental_key is not null
            then
                '{% if is_incremental() %}' || chr(10)
                || '    where ' || suggested_incremental_key || ' >= (' || chr(10)
                || '        select dateadd(day, -1, max(' || suggested_incremental_key || ')) from {{ this }}' || chr(10)
                || '    )' || chr(10)
                || '{% endif %}'
        when coalesce(suggested_strategy, 'Manual Check') = 'insert_overwrite'
            and suggested_incremental_key is not null
            then
                '{% if is_incremental() %}' || chr(10)
                || '    where ' || suggested_incremental_key || ' >= dateadd(day, -3, current_date())  -- adjust lookback as needed' || chr(10)
                || '{% endif %}'
        when coalesce(suggested_strategy, 'Manual Check') = 'append'
            and suggested_incremental_key is not null
            then
                '{% if is_incremental() %}' || chr(10)
                || '    where ' || suggested_incremental_key || ' > (select max(' || suggested_incremental_key || ') from {{ this }})' || chr(10)
                || '{% endif %}'
        else '-- No suitable incremental key detected. Add an {% if is_incremental() %} filter manually.'
    end as incremental_filter_template,
    -- copy-pasteable config() block for the dbt model
    case
        when coalesce(suggested_strategy, 'Manual Check') = 'merge'
            then
                '{{ config(' || chr(10)
                || '    materialized=''incremental'',' || chr(10)
                || '    incremental_strategy=''merge'',' || chr(10)
                || '    unique_key=''' || coalesce(suggested_unique_key, '-- TODO: add your surrogate key column') || ''',' || chr(10)
                || '    on_schema_change=''append_new_columns''' || chr(10)
                || ') }}'
        when coalesce(suggested_strategy, 'Manual Check') = 'insert_overwrite'
            then
                '{{ config(' || chr(10)
                || '    materialized=''incremental'',' || chr(10)
                || '    incremental_strategy=''insert_overwrite'',' || chr(10)
                || '    on_schema_change=''append_new_columns''' || chr(10)
                || ') }}'
        when coalesce(suggested_strategy, 'Manual Check') = 'append'
            then
                '{{ config(' || chr(10)
                || '    materialized=''incremental'',' || chr(10)
                || '    incremental_strategy=''append'',' || chr(10)
                || '    on_schema_change=''append_new_columns''' || chr(10)
                || ') }}'
        else
                '{{ config(' || chr(10)
                || '    materialized=''incremental'',' || chr(10)
                || '    incremental_strategy=''merge'',  -- verify strategy based on your data patterns' || chr(10)
                || '    unique_key=''-- TODO: add your surrogate key column'',' || chr(10)
                || '    on_schema_change=''append_new_columns''' || chr(10)
                || ') }}'
    end as updated_model_config,
    -- microbatch alternative: populated when insert-heavy + TIMESTAMP column exists
    case
        when coalesce(suggested_strategy, 'Manual Check') = 'merge'
            and best_timestamp_col is not null
            and (insert_count > (update_count + merge_count) * 9 or (update_count + merge_count) = 0)
            then
                '{{ config(' || chr(10)
                || '    materialized=''incremental'',' || chr(10)
                || '    incremental_strategy=''microbatch'',' || chr(10)
                || '    event_time=''' || best_timestamp_col || ''',' || chr(10)
                || '    begin=''YYYY-MM-DD'',  -- TODO: set your historical start date' || chr(10)
                || '    batch_size=''day'',' || chr(10)
                || '    lookback=1' || chr(10)
                || ') }}'
        else null
    end as microbatch_config_template
from scored
order by max_build_time_sec desc, table_size_gb desc
