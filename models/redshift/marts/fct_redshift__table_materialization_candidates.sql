{{
  config(
    materialized='table',
  )
}}

{#--
  Redshift port of fct_snowflake__table_materialization_candidates.

  dbt VIEW and EPHEMERAL models that are candidates for conversion to TABLE
  materialization, scored by rebuild cost (always available) plus direct
  query activity (available only when int_redshift__query_view_access can
  attribute queries to the view).

  Query attribution comes from int_redshift__query_view_access, which
  attributes via the dbt manifest graph (int_redshift__view_terminal_ancestors)
  rather than Redshift's own pg_depend catalogs or query_text matching. This
  works identically for late-binding and regular views. overlap_input_bytes/
  execution_time_seconds are summed only across the specific scan steps that
  hit a view's terminal ancestors, not the whole query, so query_activity_score
  isn't inflated by unrelated tables scanned in the same query.

  downstream_build_time_s uses the same per-step attribution mechanism,
  applied to rebuild cost instead of query activity: each downstream table's
  CTAS build is identified via the dbt node_id embedded in its query comment,
  then only the scan steps within that build which hit THIS view's terminal
  ancestors are summed — not the CTAS's whole elapsed time. It is the SUM,
  across every downstream table this view feeds, of that table's own average
  attributed build cost, averaged separately per table first and then summed
  (not one blended average across every downstream table's build events
  multiplied by a raw table count — a single expensive downstream table
  would otherwise drag up the figure applied uniformly to every other,
  cheaper table this view also feeds).

  KNOWN LIMITATION, not a bug: if two views share a terminal ancestor — e.g.
  a view built on top of another view that itself depends on the same
  source — the same scan step can legitimately get attributed to both
  views' downstream_build_time_s. This isn't double-counting to fix; it
  accurately reflects that materializing EITHER view would eliminate that
  particular rescan. It does mean rebuild_cost_score across different rows
  in this mart should not be summed as if they were disjoint contributions
  to one total cost.

  SCORING:

    rebuild_cost_score    = downstream_build_time_s   (informational)
    query_activity_score  = select_count * avg_gb_scanned_per_query * relative_duration_ratio   (informational)
    composite_chain_score = query_activity_score + downstream_build_time_s

  A plain sum. downstream_build_time_s already reflects fan-out correctly —
  it's a genuine sum across every downstream table this view feeds, weighted
  by each table's own real attributed cost — so no further multiplier is
  applied on top of it. min_hops_to_table has no role in the score: the
  cascading recomputation of an upstream view through several layers of
  inlining is already fully captured inside downstream_build_time_s (Redshift
  inlines the whole chain into one execution plan), so multiplying by hop
  count on top of that would double-count the same cost — and in the wrong
  direction, since materializing the view CLOSEST to a downstream table
  eliminates MORE cumulative upstream work than materializing one further
  away. Retained as an informational column only.

  Controlled by the following dbt variables:
    - table_materialization_min_query_count     (default 10)  — output pre-filter
    - table_materialization_min_composite_score  (default 50)  — recommendation threshold
    - table_materialization_lookback_days       (default 14, controls upstream lookback)
--#}

{% set min_query_count = var('table_materialization_min_query_count', 10) %}
{% set min_composite_score = var('table_materialization_min_composite_score', 50) %}

with view_candidates as (

    select
        lower(database_name) as database_name,
        lower(schema_name)   as schema_name,
        lower(table_name)    as table_name,
        lower(database_name) || '.' || lower(schema_name) || '.' || lower(table_name) as table_fqn,
        dbt_model,
        model_name,
        package_name,
        materialized

    from {{ ref('int_dbt__relations') }}
    where lower(materialized) in ('view', 'ephemeral')
        and lower(package_name) != 'dbt_cost_optimization_package'

),

matched_queries as (

    select
        vc.table_fqn,
        vc.database_name,
        vc.schema_name,
        vc.table_name,
        vc.dbt_model,
        vc.model_name,
        vc.package_name,
        vc.materialized,
        qva.query_id,
        cast(qva.execution_time_seconds * 1000 as bigint) as execution_time_ms,
        coalesce(qva.overlap_input_bytes, 0)              as bytes_scanned

    from view_candidates vc
    left join {{ ref('int_redshift__query_view_access') }} qva
        on  qva.database_name = vc.database_name
        and qva.view_schema   = vc.schema_name
        and qva.view_name     = vc.table_name

),

query_stats as (

    select
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        model_name,
        package_name,
        materialized,
        approximate count(distinct query_id)                                         as select_count,
        avg(execution_time_ms) / 1000.0                                              as avg_query_duration_s,
        sum(bytes_scanned) / power(1024.0, 3)                                        as total_gb_scanned

    from matched_queries
    group by
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        model_name,
        package_name,
        materialized

),

scored_stats as (

    select
        *,
        coalesce(
            total_gb_scanned / nullif(select_count::float, 0),
            0
        )                                                                            as avg_gb_scanned_per_query,
        avg_query_duration_s
            / nullif(avg(avg_query_duration_s) over (), 0)                           as relative_duration_ratio

    from query_stats

),

chain_context as (

    select
        ss.*,
        ch.downstream_table_count,
        ch.downstream_table_fqns,
        ch.min_hops_to_table,
        ch.view_name is not null                                                     as is_in_view_chain

    from scored_stats ss
    left join {{ ref('int_redshift__view_chains') }} ch
        on  ch.view_schema = ss.schema_name
        and ch.view_name   = ss.table_name

),

view_chain_downstreams as (

    -- Unnest the SUPER array into one row per (view, downstream_table_fqn).
    -- Materialize the extracted FQN as a regular varchar in this CTE so
    -- downstream joins don't reference the SUPER element directly.
    select
        view_schema,
        view_name,
        lower(trim(fqn_elem::varchar, '"'))                          as downstream_table_fqn

    from (
        select view_schema, view_name, downstream_table_fqns
        from {{ ref('int_redshift__view_chains') }}
        where downstream_table_count > 0
    ) as vc_flat,
    vc_flat.downstream_table_fqns as fqn_elem

),

downstream_dbt_models as (

    -- Resolve each downstream table's dbt node_id, so its CTAS build query
    -- can be identified precisely instead of by bare-name matching against
    -- query_text, which is prone to substring collisions between similarly
    -- named tables.
    select distinct
        lower(database_name) || '.' || lower(schema_name) || '.' || lower(table_name) as table_fqn,
        dbt_model

    from {{ ref('int_dbt__relations') }}

),

downstream_ctas_queries as (

    select distinct
        vcd.view_schema,
        vcd.view_name,
        vcd.downstream_table_fqn,
        qh.query_id

    from view_chain_downstreams vcd
    inner join downstream_dbt_models ddm
        on vcd.downstream_table_fqn = ddm.table_fqn
    inner join {{ ref('int_redshift__query_history') }} as qh
        on  lower(qh.query_text) like lower('%"node_id": "' || ddm.dbt_model || '"%')
        and lower(qh.query_type) = 'ctas'
        and lower(qh.execution_status) = 'success'
        and qh.start_time >= dateadd(month, -1, getdate())

),

downstream_ctas_scan_matches as (

    -- For each (view, downstream_table, CTAS build), sum only the scan steps
    -- that hit THIS view's own terminal ancestors — not the whole CTAS
    -- build's duration. Same per-step attribution mechanism
    -- int_redshift__query_view_access uses for query activity, applied here
    -- to rebuild cost. Note: if two views share a terminal ancestor (e.g. a
    -- view built on top of another view that itself depends on a shared
    -- source), the same scan step can legitimately be attributed to both —
    -- see the mart's header comment and materialization-strategy.md.
    select
        dcq.view_schema,
        dcq.view_name,
        dcq.downstream_table_fqn,
        dcq.query_id,
        sum(qd.duration) / 1000000.0                                 as attributed_build_seconds

    from downstream_ctas_queries dcq
    inner join {{ ref('int_redshift__view_terminal_ancestors') }} vta
        on  dcq.view_schema = vta.view_schema
        and dcq.view_name   = vta.view_name
    inner join {{ ref('int_redshift__query_detail') }} qd
        on  qd.query_id = dcq.query_id
        and lower(qd.step_name) = 'scan'
        and lower(qd.metrics_level) = 'step'
        and (   lower(qd.table_name) = vta.terminal_fqn
             or lower(qd.table_name) = vta.terminal_schema || '.' || vta.terminal_name)

    group by 1, 2, 3, 4

),

downstream_table_build_stats as (

    -- Average WITHIN each downstream table separately first. Pooling every
    -- downstream table's build events into one blended average (the prior
    -- bug) lets a single expensive downstream table drag up the "per-table"
    -- figure applied uniformly to every OTHER downstream table this view
    -- feeds too.
    select
        view_schema,
        view_name,
        downstream_table_fqn,
        avg(attributed_build_seconds) as avg_build_seconds_for_table

    from downstream_ctas_scan_matches
    group by 1, 2, 3

),

downstream_build_stats as (

    -- Sum each downstream table's own honest average — this is the view's
    -- TOTAL attributable rebuild cost across everything it feeds, weighted
    -- by each table's actual measured cost, not a blended average multiplied
    -- by a raw count that assumes every downstream table costs the same.
    select
        view_schema || '.' || view_name as model_fqn,
        sum(avg_build_seconds_for_table) as downstream_build_time_s

    from downstream_table_build_stats
    group by 1

),

composite_scored as (

    select
        cc.*,
        coalesce(dbs.downstream_build_time_s, 0)                                    as downstream_build_time_s,

        -- Informational component columns — see composite_chain_score below
        -- for how they actually combine. downstream_build_time_s is already
        -- the SUM of every downstream table's own honest average build cost
        -- (see downstream_build_stats) — it must NOT be multiplied by
        -- downstream_table_count again here, that would double-count the
        -- fan-out that summing across tables already accounts for.
        coalesce(dbs.downstream_build_time_s, 0)                                   as rebuild_cost_score,
        coalesce(cc.select_count, 0)
            * coalesce(cc.avg_gb_scanned_per_query, 0)
            * coalesce(cc.relative_duration_ratio, 1.0)                             as query_activity_score

    from chain_context cc
    left join downstream_build_stats dbs
        on  dbs.model_fqn = cc.schema_name || '.' || cc.table_name

),

final_scored as (

    select
        *,
        -- Plain sum. downstream_build_time_s is already the TOTAL attributable
        -- rebuild cost across every downstream table this view feeds (see
        -- downstream_build_stats — summed per-table, not a blended average
        -- times a raw count), and query_activity_score is already a complete
        -- direct-query cost. No further multiplier is needed or justified:
        -- fan-out is already correctly reflected inside downstream_build_time_s
        -- itself. min_hops_to_table is deliberately excluded — the cascading
        -- recomputation of an upstream view through several layers of
        -- inlining is already fully captured inside downstream_build_time_s
        -- (Redshift inlines the whole chain into one execution plan), so
        -- multiplying by hop count on top of that would double-count the same
        -- phenomenon — and in the wrong direction, since materializing the
        -- view CLOSEST to a downstream table (fewer hops) eliminates MORE
        -- cumulative upstream work than materializing one further away, not
        -- less. See materialization-strategy.md.
        query_activity_score + downstream_build_time_s                             as composite_chain_score,
        rebuild_cost_score > 0                                                      as has_rebuild_cost_signal,
        query_activity_score > 0                                                    as has_query_activity_signal

    from composite_scored

),

final as (

    select
        current_date                                                                 as snapshot_date,
        getdate()                                                                    as analyzed_at,
        fs.table_fqn,
        fs.database_name,
        fs.schema_name,
        fs.table_name,
        fs.dbt_model,
        fs.model_name,
        fs.package_name,
        fs.materialized,
        coalesce(fs.select_count, 0)                                                 as select_count,
        round(coalesce(fs.avg_query_duration_s, 0), 2)                               as avg_query_duration_s,
        round(coalesce(fs.relative_duration_ratio, 0), 4)                            as relative_duration_ratio,
        round(coalesce(fs.total_gb_scanned, 0), 4)                                   as total_gb_scanned,
        round(coalesce(fs.avg_gb_scanned_per_query, 0), 6)                           as avg_gb_scanned_per_query,
        fs.is_in_view_chain,
        fs.min_hops_to_table,
        fs.downstream_table_count,
        round(fs.rebuild_cost_score, 4)                                              as rebuild_cost_score,
        round(fs.query_activity_score, 4)                                            as query_activity_score,
        round(fs.composite_chain_score, 4)                                           as composite_chain_score,
        round(coalesce(fs.downstream_build_time_s, 0), 2)                           as downstream_build_time_s,
        fs.has_rebuild_cost_signal,
        fs.has_query_activity_signal,
        case
            when fs.composite_chain_score >= {{ min_composite_score }}
                then 'Materialize as TABLE'
            else 'Monitor'
        end                                                                          as recommendation,
        -- Explicit varchar cast: Redshift infers a fixed CTAS column width from
        -- a single CASE branch, which can be too narrow for other branches'
        -- actual data-dependent lengths — "value too long for type character
        -- varying(N)" (SQLSTATE 22001) at insert time otherwise.
        cast(
            case
                when fs.has_rebuild_cost_signal and fs.has_query_activity_signal
                    then 'Feeds ' || fs.downstream_table_count::varchar
                        || ' downstream table(s), totaling ~'
                        || round(fs.downstream_build_time_s, 1)::varchar
                        || 's of attributable rebuild cost, AND queried ' || fs.select_count::varchar
                        || ' time(s) directly, scanning ' || round(fs.avg_gb_scanned_per_query, 2)::varchar
                        || ' GB/query on average — high-confidence candidate on both signals, '
                        || 'combined before applying downstream fan-out.'
                when fs.has_rebuild_cost_signal
                    then 'Feeds ' || fs.downstream_table_count::varchar
                        || ' downstream table(s), totaling ~'
                        || round(fs.downstream_build_time_s, 1)::varchar
                        || 's of attributable rebuild cost across them (measured from CTAS history) — '
                        || 'materializing eliminates this recomputation on every downstream build. '
                        || 'No direct query-activity signal available for this view (see has_query_activity_signal).'
                when fs.has_query_activity_signal
                    then 'Queried ' || fs.select_count::varchar
                        || ' time(s) directly in the lookback window, scanning '
                        || round(fs.avg_gb_scanned_per_query, 2)::varchar
                        || ' GB/query on average ('
                        || round(fs.relative_duration_ratio, 1)::varchar
                        || 'x project-average duration) — materializing eliminates repeated '
                        || 'computation. No downstream dbt consumers detected.'
                else 'Below recommendation threshold on both rebuild-cost and query-activity '
                    || 'signals — continue monitoring'
            end
        as varchar(2000))                                                            as recommendation_reason,
        case
            when fs.has_rebuild_cost_signal and fs.has_query_activity_signal then 'high'
            when fs.has_rebuild_cost_signal or fs.has_query_activity_signal then 'medium'
            else 'low'
        end                                                                          as recommendation_confidence

    from final_scored fs
    where coalesce(fs.select_count, 0) >= {{ min_query_count }}
       or coalesce(fs.is_in_view_chain, false)

)

select * from final
order by
    case when recommendation = 'Materialize as TABLE' then 0 else 1 end,
    case recommendation_confidence when 'high' then 0 when 'medium' then 1 else 2 end,
    composite_chain_score desc
