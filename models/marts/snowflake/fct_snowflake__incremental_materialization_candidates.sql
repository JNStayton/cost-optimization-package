{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  dbt TABLE models that are strong candidates for conversion to INCREMENTAL
  materialization, based on rebuild cost, table size, and — when sufficient
  history exists — rebuild redundancy: the fraction of each full rebuild that
  re-processes unchanged rows.

  The key signal is rebuild_redundancy_rate = rows_at_period_start /
  rows_at_period_end (chronologically ordered first/last CTAS rows_inserted
  over the lookback window). A rate of 0.95 means 95% of every rebuild is
  reproducing identical data. Combined with size and frequency, this produces
  est_daily_redundant_gb_scanned — the compute waste quantified in units the
  engineer can tie back to warehouse credits.

  Recommendation tiers:
    Strong Candidate                     — rebuild_redundancy_rate >= 0.90 with reliable signal
    Candidate                            — rebuild_redundancy_rate in [0.70, 0.90) with reliable signal
    Candidate — Moderate Redundancy      — rebuild_redundancy_rate in [0.50, 0.70) with reliable signal;
                                           some overhead reduction but verify growth pattern first
    Low ROI — Minimal Rebuild Redundancy — rebuild_redundancy_rate < 0.50 with reliable signal;
                                           table grows too quickly for incremental to save meaningfully
    Candidate — Verify Growth Signal     — thresholds met but row count decreased mid-lookback
                                           (possible full-refresh or upstream deletes)
    Candidate — Insufficient History     — thresholds met but fewer than
                                           min_qualified_build_days CTAS runs recorded

  Model 2 (fct_snowflake__incremental_key_candidates) depends on this model
  and adds strategy recommendation + copy-pasteable config templates.

  Controlled by the following dbt variables:
    - incremental_candidates_lookback_days                   (default 60)
    - incremental_candidates_min_build_time_sec              (default 300)
    - incremental_candidates_min_size_gb                     (default 2)
    - incremental_candidates_min_compute_waste_score         (default 5)
    - incremental_candidates_min_qualified_build_days        (default 3)
    - incremental_candidates_min_compute_waste_avg_build_sec (default 30)
--#}

{% set lookback_days            = var('incremental_candidates_lookback_days', 60) %}
{% set min_build_time_sec       = var('incremental_candidates_min_build_time_sec', 300) %}
{% set min_size_gb              = var('incremental_candidates_min_size_gb', 2) %}
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
),

build_stats as (
    -- single scan: growth signal aggregates use a null key for non-build days so
    -- MIN_BY/MAX_BY naturally exclude them (null keys are skipped by aggregates).
    select
        table_database || '.' || table_schema || '.' || table_name            as table_fqn,
        sum(table_build_count)                                                 as table_build_count,
        max(max_build_time_ms)                                                 as max_build_time_ms,
        -- weighted average across all builds in the window (not an avg of daily avgs)
        sum(build_execution_time_ms_sum)
            / nullif(sum(table_build_count), 0)                               as avg_build_time_ms,
        sum(select_count)                                                      as select_count,
        sum(select_execution_time_ms_sum)
            / nullif(sum(select_count), 0)                                    as avg_query_execution_time_ms,
        sum(dml_count)                                                         as dml_count,
        sum(insert_count)                                                      as insert_count,
        sum(update_count)                                                      as update_count,
        sum(delete_count)                                                      as delete_count,
        sum(merge_count)                                                       as merge_count,
        -- growth signal — null key excludes non-build days from MIN_BY/MAX_BY
        count_if(rows_inserted_build_snapshot is not null)                    as qualified_build_days,
        min_by(
            rows_inserted_build_snapshot,
            case when rows_inserted_build_snapshot is not null then stats_date end
        )                                                                     as rows_at_period_start,
        max_by(
            rows_inserted_build_snapshot,
            case when rows_inserted_build_snapshot is not null then stats_date end
        )                                                                     as rows_at_period_end
    from {{ ref('int_snowflake__table_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by 1
),

table_size as (
    select
        table_fqn,
        size_gb,
        row_count
    from {{ ref('int_snowflake__table_inventory') }}
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
        coalesce(ts.size_gb, 0)                                                as size_gb,
        -- ACCOUNT_USAGE.TABLES row_count can be stale or 0 for frequently-rebuilt
        -- tables; fall back to the most recent CTAS rows_inserted snapshot when null/0
        coalesce(
            nullif(ts.row_count, 0),
            bs.rows_at_period_end,
            0
        )                                                                      as row_count,
        -- build performance
        coalesce(bs.table_build_count, 0)                                      as table_build_count,
        round(
            coalesce(bs.table_build_count, 0) / {{ lookback_days }}::float, 2
        )                                                                      as builds_per_day,
        round(coalesce(bs.max_build_time_ms, 0) / 1000.0, 1)                  as max_build_time_sec,
        round(coalesce(bs.avg_build_time_ms, 0) / 1000.0, 1)                  as avg_build_time_sec,
        -- query activity
        coalesce(bs.select_count, 0)                                           as select_count,
        round(coalesce(bs.avg_query_execution_time_ms, 0) / 1000.0, 2)        as avg_query_duration_s,
        -- dml breakdown — passed through to Model 2 for strategy inference
        coalesce(bs.dml_count, 0)                                              as dml_count,
        coalesce(bs.insert_count, 0)                                           as insert_count,
        coalesce(bs.update_count, 0)                                           as update_count,
        coalesce(bs.delete_count, 0)                                           as delete_count,
        coalesce(bs.merge_count, 0)                                            as merge_count,
        -- growth signal
        coalesce(bs.qualified_build_days, 0)                                   as qualified_build_days,
        bs.rows_at_period_start,
        bs.rows_at_period_end,
        -- rebuild_redundancy_rate: fraction of each rebuild that is unchanged rows.
        -- null when signal is unreliable or history is insufficient.
        case
            when coalesce(bs.qualified_build_days, 0) >= {{ min_qualified_build_days }}
             and bs.rows_at_period_end >= bs.rows_at_period_start
            then round(
                bs.rows_at_period_start / nullif(bs.rows_at_period_end, 0),
                4
            )
        end                                                                    as rebuild_redundancy_rate,
        -- false when row count decreased mid-lookback (possible full-refresh or upstream
        -- deletes) or when history is too thin; both make the rate untrustworthy
        case
            when coalesce(bs.qualified_build_days, 0) < {{ min_qualified_build_days }}
                then false
            when bs.rows_at_period_end < bs.rows_at_period_start
                then false
            else true
        end                                                                    as growth_signal_reliable,
        -- composite waste score: size × frequency (dimensionless ranking signal)
        round(
            coalesce(ts.size_gb, 0)
                * (coalesce(bs.table_build_count, 0) / {{ lookback_days }}::float),
            2
        )                                                                      as compute_waste_score,
        -- trigger flags — transparent about why each table was surfaced
        coalesce(bs.max_build_time_ms, 0) / 1000.0 >= {{ min_build_time_sec }} as triggered_by_build_time,
        coalesce(ts.size_gb, 0) >= {{ min_size_gb }}                           as triggered_by_size,
        (
            round(
                coalesce(ts.size_gb, 0)
                    * (coalesce(bs.table_build_count, 0) / {{ lookback_days }}::float),
                2
            ) >= {{ min_compute_waste_score }}
            and coalesce(bs.avg_build_time_ms, 0) / 1000.0 >= {{ min_compute_waste_avg_build_sec }}
        )                                                                      as triggered_by_compute_waste
    from table_candidates as tc
    left join build_stats  as bs  on bs.table_fqn  = tc.table_fqn
    left join table_size   as ts  on ts.table_fqn  = tc.table_fqn
),

final as (
    select
        *,
        case
            when growth_signal_reliable and rebuild_redundancy_rate is not null
            then round(size_gb * builds_per_day * rebuild_redundancy_rate, 2)
        end                                                                    as est_daily_redundant_gb_scanned
    from scored
)

select
    current_date()                                                             as snapshot_date,
    current_timestamp()                                                        as analyzed_at,
    {{ lookback_days }}                                                        as analysis_lookback_days,
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
    end                                                                        as recommendation,
    case
        when growth_signal_reliable and rebuild_redundancy_rate >= 0.9
            then round(rebuild_redundancy_rate * 100, 1)
                || '% of each rebuild reprocesses unchanged rows ('
                || coalesce(est_daily_redundant_gb_scanned::string, '?')
                || ' GB/day redundant) — high-ROI incremental candidate'
        when growth_signal_reliable and rebuild_redundancy_rate >= 0.7
            then round(rebuild_redundancy_rate * 100, 1)
                || '% of each rebuild reprocesses unchanged rows ('
                || coalesce(est_daily_redundant_gb_scanned::string, '?')
                || ' GB/day redundant)'
        when growth_signal_reliable and rebuild_redundancy_rate < 0.5
            then round(rebuild_redundancy_rate * 100, 1)
                || '% rebuild redundancy — table grows too quickly; incremental overhead likely '
                || 'outweighs savings'
        when growth_signal_reliable
            then round(rebuild_redundancy_rate * 100, 1)
                || '% rebuild redundancy — moderate overhead reduction, low impact; '
                || 'verify growth pattern before converting'
        when not growth_signal_reliable and qualified_build_days < {{ min_qualified_build_days }}
            then 'Only ' || qualified_build_days || ' build day(s) recorded in the '
                || {{ lookback_days }} || '-day window — growth rate requires '
                || {{ min_qualified_build_days }} || '+ build days to compute'
        when not growth_signal_reliable
            then 'Row count decreased during lookback (first: '
                || coalesce(rows_at_period_start::string, '?')
                || ' rows → last: '
                || coalesce(rows_at_period_end::string, '?')
                || ' rows) — possible full-refresh or upstream deletes; verify before acting'
        when triggered_by_build_time and triggered_by_size
            then 'Large table (' || table_size_gb
                || ' GB) with slow builds (max ' || max_build_time_sec
                || 's) — growth signal not yet available'
        else
            'Compute waste score ' || compute_waste_score
                || ' (size × builds/day) — growth signal not yet available'
    end                                                                        as recommendation_reason,
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
