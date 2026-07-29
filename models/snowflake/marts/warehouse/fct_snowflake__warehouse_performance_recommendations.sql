{{
  config(
    materialized='table',
  )
}}

{#--
  Performance recommendations for dbt-managed tables, focused on spillage with
  size-aware branching. The actionable unit is the table (which model's SQL is
  causing performance issues), not the warehouse.

  Recommendation tiers (from docs/snowflake/warehouse_recommendations_mapping.md section 4):
    4.1 Remote spillage — scale up warehouse (remote spill = severe latency)
    4.2 Heavy local spillage on small warehouse — scale up
    4.3 Heavy local spillage on large warehouse — SQL optimization needed
    4.4 Moderate spillage, worsening — monitor + SQL review
    4.5 Minor spillage, stable — no action

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

-- Find the primary warehouse that ran spilling queries for each table
-- Matches via table name in query_text for DML queries (builds that spilled)
table_warehouse as (
    select
        table_fqn,
        warehouse_name,
        row_number() over (
            partition by table_fqn
            order by total_spill desc
        ) as rn
    from (
        select
            ts.table_fqn,
            qh.warehouse_name,
            sum(qh.bytes_spilled_local + qh.bytes_spilled_remote) as total_spill
        from table_spillage_summary as ts
        inner join {{ ref('int_snowflake__query_history') }} as qh
            on qh.query_type in ('INSERT', 'MERGE', 'CREATE_TABLE_AS_SELECT')
            and qh.query_start_time >= dateadd(day, -{{ lookback_days }}, current_date())
            and (qh.bytes_spilled_local > 0 or qh.bytes_spilled_remote > 0)
            and qh.warehouse_name is not null
            and qh.query_text ilike '%' || ts.table_name || '%'
        group by 1, 2
    )
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
        coalesce(tw.warehouse_name, nullif(dr.warehouse_name, ''), 'unassigned') as warehouse_name,
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
            when ts.total_gb_spilled_local > 50
                 and lower(coalesce(wc_size.current_size, '')) in ('x-large', 'xlarge', '2x-large', '2xlarge', '3x-large', '3xlarge', '4x-large', '4xlarge')
                then 'local_heavy_large_wh'
            when ts.total_gb_spilled_local > 50
                then 'local_heavy_small_wh'
            when ts.total_gb_spilled_local > 1 and trs.spill_gb_recent > trs.spill_gb_prior * 1.2
                then 'local_moderate_worsening'
            when ts.total_gb_spilled_local > 1
                then 'local_moderate_stable'
            else 'local_minor'
        end                                                         as recommendation_key,
        coalesce(wc_size.current_size, 'unknown') as warehouse_current_size
    from table_spillage_summary as ts
    left join dbt_relations      as dr  on dr.table_fqn  = ts.table_fqn
    left join table_warehouse    as tw  on tw.table_fqn  = ts.table_fqn and tw.rn = 1
    left join trend_split        as trs on trs.table_fqn = ts.table_fqn
    left join warehouse_context  as wc  on wc.warehouse_name = coalesce(tw.warehouse_name, nullif(dr.warehouse_name, ''))
    left join {{ ref('int_snowflake__warehouse_config') }} as wc_size
        on wc_size.warehouse_name = coalesce(tw.warehouse_name, nullif(dr.warehouse_name, ''))
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
            then 'Scale up warehouse (remote spillage detected)'
        when recommendation_key = 'local_heavy_small_wh'
            then 'Scale up warehouse (heavy local spillage on ' || warehouse_current_size || ')'
        when recommendation_key = 'local_heavy_large_wh'
            then 'Optimize SQL (heavy spillage on large warehouse)'
        when recommendation_key = 'local_moderate_worsening'
            then 'Monitor — moderate spillage trending worse'
        when recommendation_key = 'local_moderate_stable'
            then 'Monitor — moderate spillage (stable)'
        else
            'Stable — minor spillage'
    end                                                             as recommendation,
    case
        when recommendation_key = 'remote_spill'
            then 'Remote spillage detected (' || total_gb_spilled_remote || ' GB in '
                || {{ lookback_days }} || ' days). Remote spill writes to cloud storage, adding significant latency and egress cost. '
                || 'Scaling up from ' || warehouse_current_size || ' provides more local SSD cache before spilling remotely. '
                || 'Trend: ' || spill_trend || '.'
        when recommendation_key = 'local_heavy_small_wh'
            then 'Heavy local spillage (' || total_gb_spilled_local || ' GB in '
                || {{ lookback_days }} || ' days) on a ' || warehouse_current_size || ' warehouse. '
                || 'Queries are exceeding available RAM and spilling to local SSD. '
                || 'Scaling up doubles available memory and will reduce or eliminate spillage. '
                || 'Trend: ' || spill_trend || '.'
        when recommendation_key = 'local_heavy_large_wh'
            then 'Heavy local spillage (' || total_gb_spilled_local || ' GB in '
                || {{ lookback_days }} || ' days) on a ' || warehouse_current_size || ' warehouse. '
                || 'At this size, further scaling has diminishing returns. '
                || 'Review query SQL for: wide JOINs missing filters, unnecessary columns in SELECT *, exploding CTEs, or missing partition pruning. '
                || 'Trend: ' || spill_trend || '.'
        when recommendation_key = 'local_moderate_worsening'
            then 'Moderate local spillage (' || total_gb_spilled_local || ' GB, trending worse). '
                || 'Profile the top spilling queries before scaling — a SQL fix (adding filters, reducing join width) is cheaper than a permanent size increase.'
        when recommendation_key = 'local_moderate_stable'
            then total_gb_spilled_local || ' GB of local spillage over '
                || {{ lookback_days }} || ' days (' || spill_days || ' spilling day(s)). '
                || 'Trend: ' || spill_trend || '. '
                || 'Tolerable but worth profiling if this model is on a critical path.'
        else
            total_gb_spilled_local || ' GB of local spillage over '
            || {{ lookback_days }} || ' days. Trend: ' || spill_trend || '. Minor — no action needed.'
    end                                                             as recommendation_reason,
    -- Concrete DDL (only for scale-up recommendations)
    case
        when recommendation_key in ('remote_spill', 'local_heavy_small_wh')
            then 'ALTER WAREHOUSE ' || warehouse_name || ' SET WAREHOUSE_SIZE = '''
                || case warehouse_current_size
                    when 'X-Small' then 'SMALL'
                    when 'Small' then 'MEDIUM'
                    when 'Medium' then 'LARGE'
                    when 'Large' then 'XLARGE'
                    when 'X-Large' then '2X-LARGE'
                    else 'MEDIUM'
                end || ''';'
        else null
    end as snowflake_ddl,
    'spillage_overflow' as symptom
from scored
order by
    case recommendation_key
        when 'remote_spill'             then 1
        when 'local_heavy_small_wh'     then 2
        when 'local_heavy_large_wh'     then 3
        when 'local_moderate_worsening' then 4
        else 5
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
                            as recommendation_reason,
    null::string            as snowflake_ddl,
    null::string            as symptom
where false

{% endif %}
