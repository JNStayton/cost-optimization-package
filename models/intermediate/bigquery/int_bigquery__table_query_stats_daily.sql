{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='table_query_stats_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  Daily query statistics per BigQuery table.

  Attribution: query text LIKE '%table_name%' matching (same fallback as Snowflake Standard
  edition). BigQuery's referenced_tables field is not in stg_bigquery__jobs_by_project.

  Column name notes (Snowflake convention; could be made neutral in future):
    - select_execution_time_ms_sum: Snowflake = wall-clock execution_time_ms
                                    BigQuery  = total_slot_ms (parallel CPU-weighted time)
    - select_partitions_scanned_sum: Snowflake = partitions scanned per query
                                     BigQuery  = 0 (not available; table-level approx_micropartitions used in scoring)
    - select_partitions_total_sum:   Snowflake = total partitions available per query
                                     BigQuery  = 0 (not available; score formula falls back to approx_micropartitions)
    - select_bytes_billed_sum:       Snowflake = 0 (not applicable; Snowflake bills by compute)
                                     BigQuery  = total_bytes_billed (primary BQ cost signal)
--#}

{% set full_account = var('table_query_stats_full_account', false) %}
{% set initial_lookback_days = var('table_query_stats_initial_lookback_days', 7) %}

with candidate_tables as (
    {% if full_account %}
    select distinct
        platform,
        database_name as table_database,
        schema_name as table_schema,
        table_name
    from {{ ref('int_bigquery__table_inventory') }}
    {% else %}
    select distinct
        ti.platform,
        ti.database_name as table_database,
        ti.schema_name as table_schema,
        ti.table_name
    from {{ ref('int_bigquery__table_inventory') }} as ti
    inner join {{ ref('int_dbt__relations') }} as dm
        on ti.database_name = dm.database_name
        and ti.schema_name = dm.schema_name
        and ti.table_name = dm.table_name
    {% endif %}
),

query_history as (
    select
        query_id,
        cast(query_start_time as date) as stats_date,
        query_start_time,
        statement_type,
        -- execution_time_ms = total_slot_ms in int_bigquery__query_history
        execution_time_ms,
        -- bytes_scanned = total_bytes_billed in int_bigquery__query_history
        bytes_scanned,
        query_text
    from {{ ref('int_bigquery__query_history') }}
    where execution_status = 'SUCCESS'
    {% if is_incremental() %}
        and query_start_time >= timestamp_sub(
            (
                select coalesce(
                    max(cast(stats_date as timestamp)),
                    cast('1970-01-01' as timestamp)
                )
                from {{ this }}
            ),
            interval 1 day
        )
    {% else %}
        and query_start_time >= timestamp_sub(
            current_timestamp(),
            interval {{ initial_lookback_days }} day
        )
    {% endif %}
),

matched_queries as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.statement_type,
        qh.execution_time_ms,
        qh.bytes_scanned
    from query_history as qh
    inner join candidate_tables as ct
        on qh.query_text like '%' || ct.table_name || '%'
)

select
    to_hex(md5(
        coalesce(platform, '') || '|' ||
        coalesce(cast(stats_date as string), '') || '|' ||
        coalesce(table_database, '') || '|' ||
        coalesce(table_schema, '') || '|' ||
        coalesce(table_name, '')
    )) as table_query_stats_daily_key,
    platform,
    stats_date,
    table_database,
    table_schema,
    table_name,
    count(*) as total_query_count,
    count(case when statement_type = 'SELECT' then 1 end) as select_count,
    count(
        case when statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
        then 1 end
    ) as dml_count,
    -- select_execution_time_ms_sum: BigQuery = total_slot_ms (parallel CPU time, not wall clock)
    sum(case when statement_type = 'SELECT' then coalesce(execution_time_ms, 0) else 0 end)
        as select_execution_time_ms_sum,
    -- select_partitions_scanned_sum: not available at query level in BigQuery JOBS_BY_PROJECT
    0 as select_partitions_scanned_sum,
    -- select_partitions_total_sum: not available; scoring falls back to approx_micropartitions
    0 as select_partitions_total_sum,
    -- select_bytes_billed_sum: BigQuery-specific primary cost signal (total_bytes_billed)
    -- In int_bigquery__query_history, bytes_scanned = total_bytes_billed
    sum(case when statement_type = 'SELECT' then coalesce(bytes_scanned, 0) else 0 end)
        as select_bytes_billed_sum,
    sum(coalesce(bytes_scanned, 0)) as bytes_scanned_sum,
    -- BigQuery has no local/remote spill concept
    0 as bytes_spilled_local_sum,
    0 as bytes_spilled_remote_sum
from matched_queries
group by 1, 2, 3, 4, 5, 6
