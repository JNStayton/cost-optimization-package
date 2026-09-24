{{
  config(
    materialized='table',
  )
}}

{#--
  Redshift port of fct_snowflake__incremental_materialization_candidates.

  dbt TABLE models that are candidates for conversion to INCREMENTAL
  materialization, based on rebuild cost, table size, and rebuild redundancy
  over a configurable lookback window.

  The key signal is rebuild_redundancy_rate = rows_at_period_start /
  rows_at_period_end (chronologically ordered first/last CTAS rows_inserted
  over the lookback window). A rate of 0.95 means 95% of every rebuild is
  reproducing identical data. Combined with size and frequency, this produces
  est_daily_redundant_gb_scanned — compute waste quantified in GB/day.

  A model is surfaced when ANY of the following triggers fire:
    - triggered_by_build_time: max rebuild time >= incremental_candidates_min_build_time_sec
      AND table size >= incremental_candidates_min_size_gb
    - triggered_by_compute_waste: compute_waste_score >= incremental_candidates_min_compute_waste_score
      AND avg build time >= incremental_candidates_min_compute_waste_avg_build_sec

  Downstream model fct_redshift__incremental_config_recommendations adds
  strategy recommendations and copy-pasteable config templates.

  Redshift-specific notes:
    - Snowflake min_by/max_by replaced with first_value window function ordered by date
    - Snowflake count_if replaced with sum(case when...)
    - CTAS build stats sourced from int_redshift__table_query_stats_daily
    - Table size/row counts sourced from int_redshift__table_inventory

  Controlled by the following dbt variables:
    - incremental_candidates_lookback_days                   (default 60)
    - incremental_candidates_min_build_time_sec              (default 300)
    - incremental_candidates_min_size_gb                     (default 2)
    - incremental_candidates_min_compute_waste_score         (default 5)
    - incremental_candidates_min_qualified_build_days        (default 3)
    - incremental_candidates_min_compute_waste_avg_build_sec (default 30)
--#}

{% set lookback_days           = var('incremental_candidates_lookback_days', 60) %}
{% set min_build_time_sec      = var('incremental_candidates_min_build_time_sec', 300) %}
{% set min_size_gb             = var('incremental_candidates_min_size_gb', 2) %}
{% set min_compute_waste_score          = var('incremental_candidates_min_compute_waste_score', 5) %}
{% set min_qualified_build_days         = var('incremental_candidates_min_qualified_build_days', 3) %}
{% set min_compute_waste_avg_build_sec  = var('incremental_candidates_min_compute_waste_avg_build_sec', 30) %}

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
        and lower(package_name) != 'dbt_cost_optimization_package'

),

build_stats_raw as (

    -- Add row numbers for picking earliest/latest CTAS snapshot per table.
    -- Replaces Snowflake min_by/max_by: ordering by date desc/asc with nulls last
    -- means rn=1 corresponds to the chronologically first or last non-null snapshot.
    select
        upper(table_database) || '.' || upper(table_schema) || '.' || upper(table_name) as table_fqn,
        stats_date,
        table_build_count,
        build_execution_time_ms_sum,
        max_build_time_ms,
        select_count,
        select_execution_time_ms_sum,
        dml_count,
        insert_count,
        update_count,
        delete_count,
        merge_count,
        rows_inserted_build_snapshot,
        row_number() over (
            partition by upper(table_database), upper(table_schema), upper(table_name)
            order by
                case when rows_inserted_build_snapshot is not null then stats_date end asc
                nulls last
        )                                                                                 as rn_asc,
        row_number() over (
            partition by upper(table_database), upper(table_schema), upper(table_name)
            order by
                case when rows_inserted_build_snapshot is not null then stats_date end desc
                nulls last
        )                                                                                 as rn_desc

    from {{ ref('int_redshift__table_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date)

),

build_stats as (

    select
        table_fqn,
        sum(table_build_count)                                                           as table_build_count,
        max(max_build_time_ms)                                                           as max_build_time_ms,
        sum(build_execution_time_ms_sum)
            / nullif(sum(table_build_count), 0)                                         as avg_build_time_ms,
        sum(select_count)                                                                as select_count,
        sum(select_execution_time_ms_sum)
            / nullif(sum(select_count), 0)                                              as avg_query_execution_time_ms,
        sum(dml_count)                                                                   as dml_count,
        sum(insert_count)                                                                as insert_count,
        sum(update_count)                                                                as update_count,
        sum(delete_count)                                                                as delete_count,
        sum(merge_count)                                                                 as merge_count,
        sum(case when rows_inserted_build_snapshot is not null then 1 else 0 end)       as qualified_build_days,
        max(case when rn_asc  = 1 then rows_inserted_build_snapshot end)                as rows_at_period_start,
        max(case when rn_desc = 1 then rows_inserted_build_snapshot end)                as rows_at_period_end

    from build_stats_raw
    group by 1

),

table_size as (

    select
        table_fqn,
        size_gb,
        row_count

    from {{ ref('int_redshift__table_inventory') }}

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
        -- size & structure
        coalesce(ts.size_gb, 0)                                                          as size_gb,
        coalesce(
            nullif(ts.row_count, 0),
            bs.rows_at_period_end,
            0
        )                                                                                as row_count,
        -- build performance
        coalesce(bs.table_build_count, 0)                                                as table_build_count,
        round(
            coalesce(bs.table_build_count, 0) / cast({{ lookback_days }} as decimal(10, 2)),
            2
        )                                                                                as builds_per_day,
        round(coalesce(bs.max_build_time_ms, 0) / 1000.0, 1)                            as max_build_time_sec,
        round(coalesce(bs.avg_build_time_ms, 0) / 1000.0, 1)                            as avg_build_time_sec,
        -- query activity
        coalesce(bs.select_count, 0)                                                     as select_count,
        round(coalesce(bs.avg_query_execution_time_ms, 0) / 1000.0, 2)                  as avg_query_duration_s,
        -- dml breakdown — passed through to fct_redshift__incremental_config_recommendations
        coalesce(bs.dml_count, 0)                                                        as dml_count,
        coalesce(bs.insert_count, 0)                                                     as insert_count,
        coalesce(bs.update_count, 0)                                                     as update_count,
        coalesce(bs.delete_count, 0)                                                     as delete_count,
        coalesce(bs.merge_count, 0)                                                      as merge_count,
        -- growth signal
        coalesce(bs.qualified_build_days, 0)                                             as qualified_build_days,
        bs.rows_at_period_start,
        bs.rows_at_period_end,
        -- rebuild_redundancy_rate: fraction of each rebuild that is unchanged rows
        case
            when coalesce(bs.qualified_build_days, 0) >= {{ min_qualified_build_days }}
             and bs.rows_at_period_end >= bs.rows_at_period_start
            then round(
                bs.rows_at_period_start / nullif(bs.rows_at_period_end, 0),
                4
            )
        end                                                                              as rebuild_redundancy_rate,
        case
            when coalesce(bs.qualified_build_days, 0) < {{ min_qualified_build_days }}
                then false
            when bs.rows_at_period_end < bs.rows_at_period_start
                then false
            else true
        end                                                                              as growth_signal_reliable,
        round(
            coalesce(ts.size_gb, 0)
                * (coalesce(bs.table_build_count, 0) / cast({{ lookback_days }} as decimal(10, 2))),
            2
        )                                                                                as compute_waste_score,
        case when coalesce(bs.max_build_time_ms, 0) / 1000.0 >= {{ min_build_time_sec }}
            then true else false end                                                     as triggered_by_build_time,
        case when coalesce(ts.size_gb, 0) >= {{ min_size_gb }}
            then true else false end                                                     as triggered_by_size,
        case
            when round(
                    coalesce(ts.size_gb, 0)
                        * (coalesce(bs.table_build_count, 0) / cast({{ lookback_days }} as decimal(10, 2))),
                    2
                ) >= {{ min_compute_waste_score }}
                and coalesce(bs.avg_build_time_ms, 0) / 1000.0 >= {{ min_compute_waste_avg_build_sec }}
            then true else false
        end                                                                              as triggered_by_compute_waste

    from table_candidates as tc
    left join build_stats  as bs on bs.table_fqn = tc.table_fqn
    left join table_size   as ts on ts.table_fqn = tc.table_fqn

),

final as (

    select
        *,
        case
            when growth_signal_reliable and rebuild_redundancy_rate is not null
            then round(size_gb * builds_per_day * rebuild_redundancy_rate, 2)
        end                                                                              as est_daily_redundant_gb_scanned

    from scored

)

select
    current_date                                                                         as snapshot_date,
    getdate()                                                                            as analyzed_at,
    {{ lookback_days }}                                                                  as analysis_lookback_days,
    table_fqn,
    database_name,
    schema_name,
    table_name,
    dbt_model,
    model_name,
    package_name,
    size_gb              as table_size_gb,
    row_count            as total_rows,
    table_build_count,
    builds_per_day,
    max_build_time_sec,
    avg_build_time_sec,
    select_count,
    avg_query_duration_s,
    dml_count,
    insert_count,
    update_count,
    delete_count,
    merge_count,
    qualified_build_days,
    rows_at_period_start,
    rows_at_period_end,
    rebuild_redundancy_rate,
    growth_signal_reliable,
    compute_waste_score,
    est_daily_redundant_gb_scanned,
    case
        when growth_signal_reliable and rebuild_redundancy_rate >= 0.9
            then 'Strong Candidate'
        when growth_signal_reliable and rebuild_redundancy_rate >= 0.7
            then 'Candidate'
        when growth_signal_reliable and rebuild_redundancy_rate >= 0.5
            then 'Candidate — Moderate Redundancy'
        when growth_signal_reliable and rebuild_redundancy_rate < 0.5
            then 'Low ROI — Minimal Rebuild Redundancy'
        when not growth_signal_reliable and qualified_build_days < {{ min_qualified_build_days }}
            then 'Candidate — Insufficient History'
        when not growth_signal_reliable
            then 'Candidate — Verify Growth Signal'
        else 'Candidate'
    end                                                                                  as recommendation,
    -- Explicit varchar cast: Redshift infers a fixed CTAS column width from a
    -- single CASE branch, which can be too narrow for other branches' actual
    -- data-dependent lengths — "value too long for type character varying(N)"
    -- (SQLSTATE 22001) at insert time otherwise.
    cast(
        case
            when growth_signal_reliable and rebuild_redundancy_rate >= 0.9
                then round(rebuild_redundancy_rate * 100, 1)::varchar
                    || '% of each rebuild reprocesses unchanged rows ('
                    || coalesce(est_daily_redundant_gb_scanned::varchar, '?')
                    || ' GB/day redundant) — high-ROI incremental candidate'
            when growth_signal_reliable and rebuild_redundancy_rate >= 0.7
                then round(rebuild_redundancy_rate * 100, 1)::varchar
                    || '% of each rebuild reprocesses unchanged rows ('
                    || coalesce(est_daily_redundant_gb_scanned::varchar, '?')
                    || ' GB/day redundant)'
            when growth_signal_reliable and rebuild_redundancy_rate < 0.5
                then round(rebuild_redundancy_rate * 100, 1)::varchar
                    || '% rebuild redundancy — table grows too quickly; incremental overhead likely '
                    || 'outweighs savings'
            when growth_signal_reliable
                then round(rebuild_redundancy_rate * 100, 1)::varchar
                    || '% rebuild redundancy — moderate overhead reduction, low impact; '
                    || 'verify growth pattern before converting'
            when not growth_signal_reliable and qualified_build_days < {{ min_qualified_build_days }}
                then 'Only ' || qualified_build_days::varchar || ' build day(s) recorded in the '
                    || {{ lookback_days }}::varchar || '-day window — growth rate requires '
                    || {{ min_qualified_build_days }}::varchar || '+ build days to compute'
            when not growth_signal_reliable
                then 'Row count decreased during lookback (first: '
                    || coalesce(rows_at_period_start::varchar, '?')
                    || ' rows → last: '
                    || coalesce(rows_at_period_end::varchar, '?')
                    || ' rows) — possible full-refresh or upstream deletes; verify before acting'
            when triggered_by_build_time and triggered_by_size
                then 'Large table (' || size_gb::varchar
                    || ' GB) with slow builds (max ' || max_build_time_sec::varchar
                    || 's) — growth signal not yet available'
            else
                'Compute waste score ' || compute_waste_score::varchar
                    || ' (size × builds/day) — growth signal not yet available'
        end
    as varchar(2000))                                                                   as recommendation_reason,
    triggered_by_build_time,
    triggered_by_size,
    triggered_by_compute_waste

from final
where triggered_by_compute_waste
   or (triggered_by_build_time and triggered_by_size)
order by
    case when est_daily_redundant_gb_scanned is not null then 0 else 1 end,
    coalesce(est_daily_redundant_gb_scanned, 0) desc,
    compute_waste_score desc
