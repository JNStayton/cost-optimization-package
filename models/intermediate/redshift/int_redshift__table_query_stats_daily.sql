{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['table_query_stats_daily_key'],
        sort='stats_date',
        on_schema_change='append_new_columns'
    )
}}

{% set full_account = var('table_query_stats_full_account', false) %}
{% set initial_lookback_days = var('table_query_stats_initial_lookback_days', 7) %}

{#
  Table identity here is matched by NAME, not table_id, for two reasons:

  1. table_id churns on every rebuild. dbt's table materialization does
     CREATE + RENAME + DROP each time, assigning a brand new table_id.
     int_redshift__table_info (sourced from svv_table_info, a current-state-
     only snapshot) only ever reflects the LATEST table_id, so joining a
     historical CTAS/scan event's table_id against it would only ever match
     the single most recent rebuild. General SELECT/DML activity
     (scan_access) is matched by table name instead, which stays the same
     across rebuilds even though table_id doesn't.

  2. A CTAS's INSERT step's table_name is the TEMP object's name, not the
     final table's — dbt's table materialization inserts into a
     __dbt_tmp-suffixed temp object first, then renames it into place. In
     the default (non-full_account) path, a CTAS's target table is instead
     identified via the dbt node_id embedded in its query comment — the same
     mechanism fct_redshift__table_materialization_candidates uses — which
     depends on neither table_id nor the insert step's (temp) table_name.
     full_account mode has no dbt node_id for non-dbt tables, so it falls
     back to the insert step's table_name with the __dbt_tmp suffix stripped
     (a no-op for genuinely non-dbt CTAS queries, which never produce that
     naming pattern).
#}

with candidate_tables as (

    {% if full_account %}

    select distinct
        'redshift'                      as platform,
        ti.database_name                as table_database,
        ti.schema_name                  as table_schema,
        ti.table_name,
        cast(null as varchar)                                                        as dbt_model,
        lower(ti.schema_name) || '.' || lower(ti.table_name)                        as table_name_fqn_2part,
        lower(ti.database_name) || '.' || lower(ti.schema_name) || '.' || lower(ti.table_name) as table_name_fqn_3part

    from {{ ref('int_redshift__table_info') }} ti

    {% else %}

    select distinct
        'redshift'                      as platform,
        dm.database_name                as table_database,
        dm.schema_name                  as table_schema,
        dm.table_name,
        dm.dbt_model,
        lower(dm.schema_name) || '.' || lower(dm.table_name)                        as table_name_fqn_2part,
        lower(dm.database_name) || '.' || lower(dm.schema_name) || '.' || lower(dm.table_name) as table_name_fqn_3part

    from {{ ref('int_dbt__relations') }} dm
    where lower(dm.materialized) in ('table', 'incremental')

    {% endif %}

),

query_history as (

    select
        query_id,
        start_time,
        cast(start_time as date)                        as stats_date,
        query_type                                      as statement_type,
        cast(elapsed_time_seconds * 1000 as bigint)     as execution_time_ms

    from {{ ref('int_redshift__query_history') }}
    where lower(execution_status) = 'success'
    {% if is_incremental() %}
        and start_time >= dateadd(
            day,
            -1,
            (select coalesce(max(stats_date), '1970-01-01'::date) from {{ this }})
        )
    {% else %}
        and start_time >= dateadd(day, -{{ initial_lookback_days }}, getdate())
    {% endif %}

),

-- One row per (query, table): sum output_bytes across all scan steps for the same table
-- in the same query (e.g. a self-join scans the same table twice — both scans count).
-- output_bytes, not input_bytes: a scan step is a leaf node in the execution
-- plan with no upstream step, so input_bytes is always 0 for it; output_bytes
-- is what the scan actually produces.
scan_access as (

    -- __dbt_tmp suffix stripped: an incremental model's merge/delete+insert
    -- build can scan its own staging temp object (created before the swap
    -- into the real table) alongside the real target — see the CTAS insert
    -- step comment below for why dbt's temp-object naming means table_name
    -- doesn't always come back as the final table's own name.
    select
        query_id,
        split_part(lower(table_name), '__dbt_tmp', 1) as table_name_matched,
        sum(coalesce(output_bytes, 0))  as bytes_scanned

    from {{ ref('int_redshift__query_detail') }}
    where step_name = 'scan'
        and table_name is not null
    group by 1, 2

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
        sa.bytes_scanned

    from scan_access sa
    inner join query_history qh
        on sa.query_id          = qh.query_id
    inner join candidate_tables ct
        on  sa.table_name_matched = ct.table_name_fqn_3part
        or  sa.table_name_matched = ct.table_name_fqn_2part

),

-- CTAS build stats: track full-rebuild cost per table per day
ctas_queries as (

    select
        query_id,
        start_time,
        cast(start_time as date)                        as ctas_date,
        cast(elapsed_time_seconds * 1000 as bigint)     as execution_time_ms,
        query_text

    from {{ ref('int_redshift__query_history') }}
    where lower(query_type) = 'ctas'
        and lower(execution_status) = 'success'

),

{% if full_account %}

ctas_insert_steps as (

    -- full_account mode sweeps in tables with no dbt node_id at all (see
    -- candidate_tables), so node_id matching (used below in the default
    -- path) isn't available here — fall back to the insert step's table_name
    -- with the __dbt_tmp suffix stripped. A no-op for genuinely non-dbt CTAS
    -- queries (which never produce that naming pattern), and correctly
    -- resolves it for any dbt-managed table swept up in the account-wide scan.
    select
        qd.query_id,
        split_part(lower(qd.table_name), '__dbt_tmp', 1) as table_name_matched,
        sum(coalesce(qd.output_rows, 0))                as rows_inserted

    from {{ ref('int_redshift__query_detail') }} qd
    inner join ctas_queries cq
        on qd.query_id = cq.query_id
    where qd.step_name = 'insert'
        and qd.table_name is not null
    group by 1, 2

),

ctas_with_rank as (

    -- row_number identifies the latest CTAS per (table, date) for snapshot extraction.
    -- Redshift rejects COUNT(DISTINCT) and first_value inside window aggregates,
    -- so we use row_number() here and GROUP BY aggregation in the next CTE.
    select
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        cq.ctas_date                                        as stats_date,
        cq.query_id,
        cq.execution_time_ms,
        cs.rows_inserted,
        row_number() over (
            partition by ct.table_database, ct.table_schema, ct.table_name, cq.ctas_date
            order by cq.start_time desc
        )                                                   as rn_latest

    from ctas_queries cq
    inner join ctas_insert_steps cs
        on cq.query_id = cs.query_id
    inner join candidate_tables ct
        on  cs.table_name_matched = ct.table_name_fqn_3part
        or  cs.table_name_matched = ct.table_name_fqn_2part

),

{% else %}

-- Default mode: every candidate table is dbt-managed, so its CTAS build is
-- identified precisely via the dbt node_id embedded in the query's own
-- comment (same mechanism fct_redshift__table_materialization_candidates
-- uses). Neither table_id (churns every rebuild) nor the insert step's
-- table_name (reflects a __dbt_tmp-suffixed temp object, not the final
-- table — dbt's table materialization inserts into a temp relation before
-- renaming it into place) can identify a historical CTAS build's target
-- reliably; node_id matching depends on neither.
ctas_query_matches as (

    select distinct
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        cq.query_id,
        cq.ctas_date,
        cq.start_time,
        cq.execution_time_ms

    from candidate_tables ct
    inner join ctas_queries cq
        on lower(cq.query_text) like lower('%"node_id": "' || ct.dbt_model || '"%')

),

ctas_insert_steps as (

    -- No table matching needed here — ctas_query_matches already resolved
    -- query_id -> table via node_id, so every insert step for a matched
    -- query_id belongs to that table's build.
    select
        query_id,
        sum(coalesce(output_rows, 0)) as rows_inserted

    from {{ ref('int_redshift__query_detail') }}
    where step_name = 'insert'
    group by 1

),

ctas_with_rank as (

    select
        cqm.table_database,
        cqm.table_schema,
        cqm.table_name,
        cqm.ctas_date                                        as stats_date,
        cqm.query_id,
        cqm.execution_time_ms,
        cs.rows_inserted,
        row_number() over (
            partition by cqm.table_database, cqm.table_schema, cqm.table_name, cqm.ctas_date
            order by cqm.start_time desc
        )                                                   as rn_latest

    from ctas_query_matches cqm
    left join ctas_insert_steps cs
        on cqm.query_id = cs.query_id

),

{% endif %}

ctas_per_table_date as (

    select
        table_database,
        table_schema,
        table_name,
        stats_date,
        count(distinct query_id)                            as table_build_count,
        sum(execution_time_ms)                              as build_execution_time_ms_sum,
        max(execution_time_ms)                              as max_build_time_ms,
        max(case when rn_latest = 1 then rows_inserted end) as rows_inserted_build_snapshot

    from ctas_with_rank
    group by 1, 2, 3, 4

),

daily_query_stats as (

    -- Pre-aggregate matched_queries to one row per (table, date) BEFORE
    -- joining to ctas_per_table_date, so neither side has to "win" as the
    -- driving table — see all_table_dates below.
    select
        table_database,
        table_schema,
        table_name,
        stats_date,
        count(*)                                                        as total_query_count,
        count(case when statement_type = 'SELECT' then 1 end)           as select_count,
        count(case when statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE') then 1 end)
                                                                        as dml_count,
        count(case when statement_type = 'INSERT' then 1 end)           as insert_count,
        count(case when statement_type = 'UPDATE' then 1 end)           as update_count,
        count(case when statement_type = 'DELETE' then 1 end)           as delete_count,
        count(case when statement_type = 'MERGE' then 1 end)            as merge_count,
        sum(case when statement_type = 'SELECT'
            then coalesce(execution_time_ms, 0) else 0 end)             as select_execution_time_ms_sum,
        sum(coalesce(bytes_scanned, 0))                                  as bytes_scanned_sum

    from matched_queries
    group by 1, 2, 3, 4

),

all_table_dates as (

    -- A table can have a CTAS build on a date with zero matched SELECT/DML
    -- query activity that same day (e.g. a nightly-only rebuild) — union the
    -- key sets from both sides so that build never gets silently dropped for
    -- lack of a matching row on the query-activity side.
    select table_database, table_schema, table_name, stats_date from daily_query_stats
    union
    select table_database, table_schema, table_name, stats_date from ctas_per_table_date

)

select
    md5(
        'redshift'                                       || '|' ||
        coalesce(cast(atd.stats_date as varchar), '')    || '|' ||
        coalesce(atd.table_database, '')                 || '|' ||
        coalesce(atd.table_schema, '')                   || '|' ||
        coalesce(atd.table_name, '')
    )                                                               as table_query_stats_daily_key,
    'redshift'                                                       as platform,
    atd.stats_date,
    atd.table_database,
    atd.table_schema,
    atd.table_name,
    coalesce(dqs.total_query_count, 0)                              as total_query_count,
    coalesce(dqs.select_count, 0)                                   as select_count,
    coalesce(dqs.dml_count, 0)                                      as dml_count,
    coalesce(dqs.insert_count, 0)                                   as insert_count,
    coalesce(dqs.update_count, 0)                                   as update_count,
    coalesce(dqs.delete_count, 0)                                   as delete_count,
    coalesce(dqs.merge_count, 0)                                    as merge_count,
    coalesce(dqs.select_execution_time_ms_sum, 0)                   as select_execution_time_ms_sum,
    cast(0 as bigint)                                               as select_partitions_scanned_sum,
    cast(0 as bigint)                                               as select_partitions_total_sum,
    coalesce(dqs.bytes_scanned_sum, 0)                              as bytes_scanned_sum,
    cast(0 as bigint)                                               as bytes_spilled_local_sum,
    cast(0 as bigint)                                               as bytes_spilled_remote_sum,
    -- CTAS build stats (null when no CTAS runs target this table on this date)
    b.table_build_count,
    b.build_execution_time_ms_sum,
    b.max_build_time_ms,
    b.rows_inserted_build_snapshot

from all_table_dates atd
left join daily_query_stats dqs
    on  atd.table_database  = dqs.table_database
    and atd.table_schema    = dqs.table_schema
    and atd.table_name      = dqs.table_name
    and atd.stats_date      = dqs.stats_date
left join ctas_per_table_date b
    on  atd.table_database  = b.table_database
    and atd.table_schema    = b.table_schema
    and atd.table_name      = b.table_name
    and atd.stats_date      = b.stats_date
