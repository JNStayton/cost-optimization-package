{{
  config(
    materialized='table',
  )
}}

{#--
  Spillage recommendations for dbt-managed tables, with warehouse-level trend
  context. The actionable unit is the table (which model's SQL is spilling),
  not the warehouse — fixing the query is almost always cheaper than scaling up.

  Recommendation tiers (same as find_spillage_candidates macro):
    Critical — Remote Spillage  (warn) — any meaningful remote spillage (> 0.1 GB)
    Warn — Heavy Local Spillage (warn) — local spillage > 5 GB total in window
    Monitor — Moderate Spillage (info) — above floor but not severe

  Trend direction (30-day window, comparing first 15 days to last 15 days):
    Worsening  — recent 15-day spill > prior 15-day spill by more than 20%
    Improving  — recent 15-day spill < prior 15-day spill by more than 20%
    Stable     — within 20% of prior period

  Requires Snowflake Enterprise Edition (snowflake_enterprise_edition = true).
  When snowflake_enterprise_edition = false, table-level attribution is not
  available and this model will produce no rows.

  Controlled by the following dbt variables:
    - spillage_lookback_days          (default 30)
    - spillage_min_total_gb           (default 0.05)
    - spillage_min_runs               (default 1)
--#}

{% set is_enterprise  = var('snowflake_enterprise_edition', true) %}
{% set lookback_days       = var('spillage_lookback_days', 30) %}
{% set min_total_gb        = var('spillage_min_total_gb', 0.05) %}
{% set min_runs            = var('spillage_min_runs', 1) %}
{% set trend_split_days    = (lookback_days / 2) | int %}

{% if is_enterprise %}

with table_spillage as (
    select
        upper(tqs.table_database) || '.' || upper(tqs.table_schema) || '.' || upper(tqs.table_name)
                                                                    as table_fqn,
        tqs.table_database,
        tqs.table_schema,
        tqs.table_name,
        tqs.stats_date,
        tqs.bytes_spilled_local_sum,
        tqs.bytes_spilled_remote_sum,
        tqs.dml_count,
        tqs.table_build_count
    from {{ ref('int_snowflake__table_query_stats_daily') }} as tqs
    where tqs.stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
      and (tqs.bytes_spilled_local_sum > 0 or tqs.bytes_spilled_remote_sum > 0)
),

table_spillage_summary as (
    select
        table_fqn,
        table_database,
        table_schema,
        table_name,
        count(distinct stats_date)                                  as spill_days,
        sum(dml_count + table_build_count)                          as total_runs,
        round(sum(bytes_spilled_local_sum) / power(1024, 3), 4)    as total_gb_spilled_local,
        round(sum(bytes_spilled_remote_sum) / power(1024, 3), 4)   as total_gb_spilled_remote,
        round(avg(bytes_spilled_local_sum) / power(1024, 3), 4)    as avg_gb_spilled_local_per_day,
        round(avg(bytes_spilled_remote_sum) / power(1024, 3), 4)   as avg_gb_spilled_remote_per_day
    from table_spillage
    group by table_fqn, table_database, table_schema, table_name
    having
        sum(dml_count + table_build_count) >= {{ min_runs }}
        and (
            round(sum(bytes_spilled_local_sum) / power(1024, 3), 4)
            + round(sum(bytes_spilled_remote_sum) / power(1024, 3), 4)
        ) >= {{ min_total_gb }}
),

trend_split as (
    select
        upper(table_database) || '.' || upper(table_schema) || '.' || upper(table_name)
                                                                    as table_fqn,
        round(
            sum(case when stats_date < dateadd(day, -{{ trend_split_days }}, current_date())
                then bytes_spilled_local_sum + bytes_spilled_remote_sum else 0 end)
            / power(1024, 3), 4
        )                                                           as spill_gb_prior,
        round(
            sum(case when stats_date >= dateadd(day, -{{ trend_split_days }}, current_date())
                then bytes_spilled_local_sum + bytes_spilled_remote_sum else 0 end)
            / power(1024, 3), 4
        )                                                           as spill_gb_recent
    from {{ ref('int_snowflake__table_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
      and (bytes_spilled_local_sum > 0 or bytes_spilled_remote_sum > 0)
    group by 1
),

warehouse_context as (
    select
        warehouse_name,
        sum(spilling_query_count)                                   as warehouse_spill_days_30d,
        round(sum(total_gb_spilled_local + total_gb_spilled_remote), 2)
                                                                    as warehouse_total_gb_spilled_30d
    from {{ ref('int_snowflake__warehouse_spillage_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by warehouse_name
),

dbt_relations as (
    select
        upper(database_name) || '.' || upper(schema_name) || '.' || upper(table_name)
                                                                    as table_fqn,
        dbt_model,
        model_name,
        materialized,
        warehouse_name
    from {{ ref('int_dbt__relations') }}
),

scored as (
    select
        ts.table_fqn,
        ts.table_database,
        ts.table_schema,
        ts.table_name,
        dr.dbt_model,
        dr.model_name,
        dr.materialized                                             as current_materialization,
        coalesce(nullif(dr.warehouse_name, ''), 'unassigned')        as warehouse_name,
        ts.spill_days,
        ts.total_runs,
        ts.total_gb_spilled_local,
        ts.total_gb_spilled_remote,
        ts.avg_gb_spilled_local_per_day,
        ts.avg_gb_spilled_remote_per_day,
        coalesce(trs.spill_gb_recent, 0)                            as spill_gb_recent_15d,
        coalesce(trs.spill_gb_prior, 0)                             as spill_gb_prior_15d,
        case
            {% if lookback_days <= 2 %}
            'Insufficient data'
            {% else %}
            when coalesce(trs.spill_gb_prior, 0) = 0
                 and coalesce(trs.spill_gb_recent, 0) = 0
                then 'Stable'
            when coalesce(trs.spill_gb_prior, 0) = 0
                 and coalesce(trs.spill_gb_recent, 0) > 0
                then 'Worsening'
            when coalesce(trs.spill_gb_recent, 0)
                 > coalesce(trs.spill_gb_prior, 0) * 1.2
                then 'Worsening'
            when coalesce(trs.spill_gb_recent, 0)
                 < coalesce(trs.spill_gb_prior, 0) * 0.8
                then 'Improving'
            else 'Stable'
            {% endif %}
        end                                                         as spill_trend,
        coalesce(wc.warehouse_spill_days_30d, 0)                   as warehouse_spill_days_30d,
        coalesce(wc.warehouse_total_gb_spilled_30d, 0)             as warehouse_total_gb_spilled_30d,
        case
            when ts.total_gb_spilled_remote > 0.1
                then 'remote_spill'
            when ts.total_gb_spilled_local > 5
                then 'local_heavy'
            else 'local_moderate'
        end                                                         as recommendation_key
    from table_spillage_summary as ts
    left join dbt_relations      as dr  on dr.table_fqn  = ts.table_fqn
    left join trend_split        as trs on trs.table_fqn = ts.table_fqn
    left join warehouse_context  as wc  on wc.warehouse_name = dr.warehouse_name
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    dbt_model,
    model_name,
    current_materialization,
    warehouse_name,
    spill_days,
    total_runs,
    total_gb_spilled_local,
    total_gb_spilled_remote,
    avg_gb_spilled_local_per_day,
    avg_gb_spilled_remote_per_day,
    spill_gb_recent_15d,
    spill_gb_prior_15d,
    spill_trend,
    warehouse_spill_days_30d,
    warehouse_total_gb_spilled_30d,
    case
        when recommendation_key = 'remote_spill'
            then 'Critical — Refactor SQL or scale up warehouse (remote spillage)'
        when recommendation_key = 'local_heavy'
            then 'Warn — Refactor SQL or scale up warehouse (heavy local spillage)'
        else
            'Monitor — Moderate local spillage'
    end                                                             as recommendation,
    case
        when recommendation_key = 'remote_spill'
            then total_gb_spilled_remote || ' GB of remote spillage over '
                || {{ lookback_days }} || ' days (' || spill_days || ' spilling day(s)) — '
                || 'compute exhausted RAM and fell back to S3, dramatically increasing '
                || 'elapsed time and credit consumption. '
                || 'Trend: ' || spill_trend || '. '
                || 'SQL-side fixes (reduce intermediate result set size, push filters '
                || 'earlier, avoid cross joins) are usually cheaper than scaling up. '
                || 'Warehouse ' || warehouse_name || ' had '
                || warehouse_spill_days_30d || ' spilling query(s) totalling '
                || warehouse_total_gb_spilled_30d || ' GB in the same window.'
        when recommendation_key = 'local_heavy'
            then total_gb_spilled_local || ' GB of local spillage over '
                || {{ lookback_days }} || ' days (' || spill_days || ' spilling day(s)) — '
                || 'intermediate results exceeded RAM. '
                || 'Trend: ' || spill_trend || '. '
                || 'SQL-side fixes are usually cheaper than scaling up. '
                || 'Warehouse ' || warehouse_name || ' had '
                || warehouse_spill_days_30d || ' spilling query(s) in the same window.'
        else
            total_gb_spilled_local || ' GB of local spillage over '
            || {{ lookback_days }} || ' days (' || spill_days || ' spilling day(s)). '
            || 'Trend: ' || spill_trend || '. '
            || 'Tolerable but worth profiling if this model is on a critical path.'
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'remote_spill'   then 1
        when 'local_heavy'    then 2
        else 3
    end,
    total_gb_spilled_remote desc,
    total_gb_spilled_local desc

{% else %}

select
    current_date()          as snapshot_date,
    current_timestamp()     as analyzed_at,
    {{ lookback_days }}     as analysis_lookback_days,
    null::string            as table_fqn,
    null::string            as table_database,
    null::string            as table_schema,
    null::string            as table_name,
    null::string            as dbt_model,
    null::string            as model_name,
    null::string            as current_materialization,
    null::string            as warehouse_name,
    null::int               as spill_days,
    null::int               as total_runs,
    null::float             as total_gb_spilled_local,
    null::float             as total_gb_spilled_remote,
    null::float             as avg_gb_spilled_local_per_day,
    null::float             as avg_gb_spilled_remote_per_day,
    null::float             as spill_gb_recent_15d,
    null::float             as spill_gb_prior_15d,
    null::string            as spill_trend,
    null::int               as warehouse_spill_days_30d,
    null::float             as warehouse_total_gb_spilled_30d,
    'Not available — requires Enterprise edition (snowflake_enterprise_edition = true)'
                            as recommendation,
    'ACCESS_HISTORY is required for table-level spillage attribution. '
    || 'Set snowflake_enterprise_edition = true in dbt_project.yml vars '
    || 'to enable this model (requires Snowflake Enterprise Edition or higher).'
                            as recommendation_reason
where false

{% endif %}
