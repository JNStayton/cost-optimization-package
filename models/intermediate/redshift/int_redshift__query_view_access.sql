{#
  Attributes SELECT queries to dbt-managed views. One row per (view, query_id).
  This is the Redshift equivalent of Snowflake's ACCESS_HISTORY-based
  view-to-query attribution.

  MECHANISM:
    1. int_redshift__view_terminal_ancestors gives each view's full set of
       terminal base tables (source() tables or table/incremental dbt
       models), resolved from the dbt manifest graph rather than Redshift's
       pg_depend/pg_rewrite catalogs — those never populate for late-binding
       views (bind=false, a documented dbt-redshift production pattern),
       which the manifest graph is unaffected by.
    2. sys_query_detail's scan steps (metrics_level = 'step', step_name =
       'scan') expose table_name per physical scan, independent of
       table_id/pg_depend resolution — this works even for views built over
       external/shared-storage tables, where table_id is not populated (AWS
       documents table_id as specific to permanent table scans).
    3. A query is attributed to a view when ALL of that view's terminal
       ancestors were scanned within that query_id.
    4. Bytes and duration are summed only across the matched scan steps, not
       the whole query, so a view's attributed cost isn't inflated by
       unrelated tables scanned in the same query (e.g. a join against a
       large fact table doesn't inflate a small dimension view's cost).

  table_name matching tries both a full database.schema.table form and a
  bare schema.table form, since Redshift doesn't always qualify table_name
  with the database segment for every object type.

  Lookback window is controlled by the materialization_lookback_days var
  (default 7 days).
#}

with query_window as (

    select
        query_id,
        start_time

    from {{ ref('int_redshift__query_history') }}
    where lower(query_type) = 'select'
        and lower(execution_status) = 'success'
        and start_time >= dateadd(day, -{{ var('materialization_lookback_days', 7) }}, getdate())

),

scan_steps as (

    select
        qd.query_id,
        lower(qd.table_name) as table_name,
        qd.output_bytes,
        qd.duration

    from {{ ref('int_redshift__query_detail') }} qd
    inner join query_window qw
        on qd.query_id = qw.query_id
    where lower(qd.step_name) = 'scan'
        and lower(qd.metrics_level) = 'step'
        and qd.table_name is not null

),

view_terminal_counts as (

    select
        database_name,
        view_schema,
        view_name,
        count(distinct terminal_fqn) as total_terminal_count

    from {{ ref('int_redshift__view_terminal_ancestors') }}
    group by 1, 2, 3

),

scan_matches as (

    select
        vta.database_name,
        vta.view_schema,
        vta.view_name,
        ss.query_id,
        count(distinct vta.terminal_fqn)  as matched_terminal_count,
        sum(ss.output_bytes)              as matched_output_bytes,
        sum(ss.duration)                  as matched_duration_microseconds

    from {{ ref('int_redshift__view_terminal_ancestors') }} vta
    inner join scan_steps ss
        on  ss.table_name = vta.terminal_fqn
        or  ss.table_name = vta.terminal_schema || '.' || vta.terminal_name
    group by 1, 2, 3, 4

),

view_access as (

    -- A query is attributed to a view only when it scanned ALL of that
    -- view's terminal ancestors — the manifest-based equivalent of the old
    -- design's scan-set corroboration.
    select
        sm.database_name,
        sm.view_schema,
        sm.view_name,
        sm.query_id,
        sm.matched_output_bytes,
        sm.matched_duration_microseconds

    from scan_matches sm
    inner join view_terminal_counts vtc
        on  sm.database_name       = vtc.database_name
        and sm.view_schema         = vtc.view_schema
        and sm.view_name           = vtc.view_name
        and sm.matched_terminal_count = vtc.total_terminal_count

)

select
    va.database_name,
    va.view_schema,
    va.view_name,
    va.query_id,
    va.matched_output_bytes                          as overlap_input_bytes,
    va.matched_duration_microseconds / 1000000.0      as execution_time_seconds,
    qw.start_time

from view_access va
inner join query_window qw
    on va.query_id = qw.query_id
