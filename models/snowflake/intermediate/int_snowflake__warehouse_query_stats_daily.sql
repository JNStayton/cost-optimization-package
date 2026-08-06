{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='warehouse_query_stats_daily_key',
    cluster_by=['stats_date'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily query performance and concurrency stats per warehouse, scoped to dbt
  sessions only. This is the primary sizing signal for
  fct_snowflake__warehouse_sizing_recommendations.

  Grain: one row per (warehouse_name, stats_date).

  Key signals:
    - dml_ratio:              fraction of queries that are DML (gen2 trigger)
    - median_overload_ms:     concurrency bottleneck signal (mcw / scale-up trigger)
    - median_provisioning_ms: auto-suspend tradeoff signal
    - queries_at_100pct_load: saturation signal (warehouse fully loaded)
    - avg_query_load_pct:     overall utilization (scale-down signal when low)
--#}

with dbt_queries as (
    select
        qh.query_id,
        cast(qh.query_start_time as date)   as stats_date,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.total_elapsed_time_ms,
        qh.queued_overload_time_ms,
        qh.queued_provisioning_time_ms,
        qh.execution_time_ms,
        qh.query_load_percent,
        qh.query_type
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_snowflake__dbt_sessions') }} as s
        on qh.session_id = s.session_id
    where qh.warehouse_name is not null
        and coalesce(qh.warehouse_size, '') != 'ADAPTIVE'
    {% if is_incremental() %}
        and qh.query_start_time >= dateadd(
            day,
            -1,
            (
                select coalesce(max(stats_date), '1970-01-01'::date)
                from {{ this }}
            )
        )
    {% else %}
        and qh.query_start_time >= dateadd(day, -30, current_timestamp())
    {% endif %}
)

select
    md5(
        coalesce(to_varchar(stats_date), '') || '|' ||
        coalesce(warehouse_name, '')
    )                                                               as warehouse_query_stats_daily_key,
    stats_date,
    warehouse_name,
    any_value(warehouse_size)                                       as warehouse_size,
    count(*)                                                        as total_dbt_queries,
    count(case when query_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE') then 1 end)
                                                                    as dml_count,
    round(
        count(case when query_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE') then 1 end)
            * 1.0 / nullif(count(*), 0),
        4
    )                                                               as dml_ratio,
    round(median(total_elapsed_time_ms), 0)                         as median_elapsed_ms,
    round(median(execution_time_ms), 0)                             as median_execution_ms,
    round(median(queued_overload_time_ms), 0)                       as median_overload_ms,
    round(median(queued_provisioning_time_ms), 0)                    as median_provisioning_ms,
    round(
        median(queued_overload_time_ms)
            / nullif(median(total_elapsed_time_ms), 0),
        4
    )                                                               as overload_to_elapsed_ratio,
    round(avg(case when query_load_percent > 0 then query_load_percent end), 2)
                                                                    as avg_query_load_pct,
    count(case when query_load_percent = 100 then 1 end)            as queries_at_100pct_load
from dbt_queries
group by stats_date, warehouse_name
