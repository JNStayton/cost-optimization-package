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

  Attribution: JOBS_BY_PROJECT.referenced_tables (engine-populated, one row per table a
  job actually read) for reads, JOBS_BY_PROJECT.destination_table for writes. This
  replaced a query-text LIKE '%table_name%' heuristic that both over-matched (any query
  whose text happened to contain a table name as a substring, e.g. dbt's query-comment
  metadata from unrelated models built in the same invocation) and under-matched
  (query_text truncation). referenced_tables/destination_table give exact attribution
  instead.

  A job's own destination_table is excluded from its reads: for write statement types,
  the target table appears in referenced_tables too (e.g. MERGE reads and writes the
  same table), and counting that as a read would conflate the two signals the
  select_count/dml_count split is meant to keep apart.

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
        -- int_dbt__relations always upper()s identifiers (a Snowflake convention);
        -- BigQuery identifiers are case-sensitive and typically lowercase, so compare
        -- case-insensitively rather than assuming a case convention.
        on upper(ti.database_name) = dm.database_name
        and upper(ti.schema_name) = dm.schema_name
        and upper(ti.table_name) = dm.table_name
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
        referenced_tables,
        destination_table
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

-- One row per (job, referenced table). A write statement's own destination table is
-- excluded here — see header note — so it only shows up in `writes` below.
reads as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.query_id,
        qh.execution_time_ms,
        qh.bytes_scanned
    from query_history as qh
    cross join unnest(qh.referenced_tables) as rt
    inner join candidate_tables as ct
        on lower(rt.project_id) = lower(ct.table_database)
        and lower(rt.dataset_id) = lower(ct.table_schema)
        and lower(rt.table_id) = lower(ct.table_name)
    where not (
        qh.statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
        and rt.project_id = qh.destination_table.project_id
        and rt.dataset_id = qh.destination_table.dataset_id
        and rt.table_id = qh.destination_table.table_id
    )
),

-- One row per write job, keyed to its destination table only.
writes as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.query_id
    from query_history as qh
    inner join candidate_tables as ct
        on lower(qh.destination_table.project_id) = lower(ct.table_database)
        and lower(qh.destination_table.dataset_id) = lower(ct.table_schema)
        and lower(qh.destination_table.table_id) = lower(ct.table_name)
    where qh.statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
),

reads_agg as (
    select
        platform,
        stats_date,
        table_database,
        table_schema,
        table_name,
        count(distinct query_id) as select_count,
        sum(coalesce(execution_time_ms, 0)) as select_execution_time_ms_sum,
        sum(coalesce(bytes_scanned, 0)) as select_bytes_billed_sum
    from reads
    group by 1, 2, 3, 4, 5
),

writes_agg as (
    select
        platform,
        stats_date,
        table_database,
        table_schema,
        table_name,
        count(distinct query_id) as dml_count
    from writes
    group by 1, 2, 3, 4, 5
),

combined as (
    select
        coalesce(r.platform, w.platform) as platform,
        coalesce(r.stats_date, w.stats_date) as stats_date,
        coalesce(r.table_database, w.table_database) as table_database,
        coalesce(r.table_schema, w.table_schema) as table_schema,
        coalesce(r.table_name, w.table_name) as table_name,
        coalesce(r.select_count, 0) as select_count,
        coalesce(w.dml_count, 0) as dml_count,
        coalesce(r.select_execution_time_ms_sum, 0) as select_execution_time_ms_sum,
        coalesce(r.select_bytes_billed_sum, 0) as select_bytes_billed_sum
    from reads_agg as r
    full outer join writes_agg as w
        on r.platform = w.platform
        and r.stats_date = w.stats_date
        and r.table_database = w.table_database
        and r.table_schema = w.table_schema
        and r.table_name = w.table_name
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
    select_count + dml_count as total_query_count,
    select_count,
    dml_count,
    -- select_execution_time_ms_sum: BigQuery = total_slot_ms (parallel CPU time, not wall clock)
    select_execution_time_ms_sum,
    -- select_partitions_scanned_sum: not available at query level in BigQuery JOBS_BY_PROJECT
    0 as select_partitions_scanned_sum,
    -- select_partitions_total_sum: not available; scoring falls back to approx_micropartitions
    0 as select_partitions_total_sum,
    -- select_bytes_billed_sum: BigQuery-specific primary cost signal (total_bytes_billed),
    -- attributed only from reads (see header note on why writes are kept separate)
    select_bytes_billed_sum,
    select_bytes_billed_sum as bytes_scanned_sum,
    -- BigQuery has no local/remote spill concept
    0 as bytes_spilled_local_sum,
    0 as bytes_spilled_remote_sum
from combined
