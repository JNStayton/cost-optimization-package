{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='expensive_query_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Daily per-query-hash credit consumption for dbt-authored queries.
  Provides the cost signal for fct_snowflake__expensive_query_recommendations.

  Grain: one row per (query_hash, stats_date).

  Credit attribution:
    - Primary (Enterprise+): query_attribution_history.credits_attributed_compute
      Controlled by var use_query_attribution (default true).
    - Fallback (Standard): credits estimated from warehouse_metering_history
      prorated by query elapsed time share within the warehouse-hour.
      This is a rough approximation — flag it clearly in the mart.

  The dbt node_id is extracted from the JSON comment dbt prepends to compiled
  queries (e.g., /* {"app": "dbt", "dbt_version": "...", "node_id": "..."} */).
--#}

{% set use_query_attribution = var('use_query_attribution', true) %}

{% if use_query_attribution %}

with dbt_queries as (
    select
        qh.query_id,
        qh.query_hash,
        cast(qh.query_start_time as date)                           as stats_date,
        qh.query_start_time,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.total_elapsed_time_ms,
        qh.query_text,
        parse_json(
            regexp_substr(qh.query_text, '/\\*\\s+(\\{.*?\\})\\s+\\*/', 1, 1, 'e', 1)
        ):node_id::string                                           as dbt_node_id
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_snowflake__dbt_sessions') }} as s
        on qh.session_id = s.session_id
    where qh.query_hash is not null
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
),

query_credits as (
    select
        dq.query_id,
        dq.query_hash,
        dq.stats_date,
        dq.warehouse_name,
        dq.warehouse_size,
        dq.total_elapsed_time_ms,
        dq.dbt_node_id,
        qah.credits_attributed_compute
    from dbt_queries as dq
    inner join {{ source('snowflake_usage', 'query_attribution_history') }} as qah
        on dq.query_id = qah.query_id
    where qah.credits_attributed_compute > 0
)

select
    md5(
        coalesce(to_varchar(stats_date), '') || '|' ||
        coalesce(query_hash, '')
    )                                                               as expensive_query_daily_key,
    stats_date,
    query_hash,
    any_value(dbt_node_id)                                          as dbt_node_id,
    any_value(warehouse_name)                                       as warehouse_name,
    any_value(warehouse_size)                                       as warehouse_size,
    count(distinct query_id)                                        as total_runs,
    round(avg(total_elapsed_time_ms) / 1000.0, 2)                  as avg_elapsed_sec,
    round(sum(credits_attributed_compute), 6)                       as total_credits,
    true                                                            as credits_from_attribution
from query_credits
group by stats_date, query_hash

{% else %}

{#--
  Standard edition fallback: prorate warehouse credits by each query's share
  of total elapsed time within the warehouse-hour bucket.

  Approximation caveats:
    - Assumes all queries in a warehouse-hour share credits proportionally to
      elapsed time, which ignores query complexity and concurrency effects.
    - credits_from_attribution = false flags this in the mart so users know
      the cost estimate is approximate.
--#}

with dbt_queries as (
    select
        qh.query_id,
        qh.query_hash,
        cast(qh.query_start_time as date)                           as stats_date,
        qh.query_start_time,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.total_elapsed_time_ms,
        date_trunc('hour', qh.query_start_time)                     as warehouse_hour,
        parse_json(
            regexp_substr(qh.query_text, '/\\*\\s+(\\{.*?\\})\\s+\\*/', 1, 1, 'e', 1)
        ):node_id::string                                           as dbt_node_id
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_snowflake__dbt_sessions') }} as s
        on qh.session_id = s.session_id
    where qh.query_hash is not null
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
),

warehouse_hour_totals as (
    select
        qh.warehouse_name,
        date_trunc('hour', qh.query_start_time)                     as warehouse_hour,
        sum(qh.total_elapsed_time_ms)                               as total_elapsed_ms_in_hour
    from {{ ref('int_snowflake__query_history') }} as qh
    where qh.warehouse_name is not null
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
    group by qh.warehouse_name, date_trunc('hour', qh.query_start_time)
),

metering_hourly as (
    select
        warehouse_name,
        date_trunc('hour', start_time)                              as warehouse_hour,
        sum(credits_used_compute)                                   as credits_in_hour
    from {{ ref('stg_snowflake__warehouse_metering_history') }}
    group by warehouse_name, date_trunc('hour', start_time)
),

query_credits as (
    select
        dq.query_id,
        dq.query_hash,
        dq.stats_date,
        dq.warehouse_name,
        dq.warehouse_size,
        dq.total_elapsed_time_ms,
        dq.dbt_node_id,
        coalesce(mh.credits_in_hour, 0)
            * (dq.total_elapsed_time_ms
                / nullif(wht.total_elapsed_ms_in_hour, 0))         as credits_attributed_compute
    from dbt_queries as dq
    left join warehouse_hour_totals as wht
        on dq.warehouse_name = wht.warehouse_name
        and dq.warehouse_hour = wht.warehouse_hour
    left join metering_hourly as mh
        on dq.warehouse_name = mh.warehouse_name
        and dq.warehouse_hour = mh.warehouse_hour
)

select
    md5(
        coalesce(to_varchar(stats_date), '') || '|' ||
        coalesce(query_hash, '')
    )                                                               as expensive_query_daily_key,
    stats_date,
    query_hash,
    any_value(dbt_node_id)                                          as dbt_node_id,
    any_value(warehouse_name)                                       as warehouse_name,
    any_value(warehouse_size)                                       as warehouse_size,
    count(distinct query_id)                                        as total_runs,
    round(avg(total_elapsed_time_ms) / 1000.0, 2)                  as avg_elapsed_sec,
    round(sum(credits_attributed_compute), 6)                       as total_credits,
    false                                                           as credits_from_attribution
from query_credits
where credits_attributed_compute > 0
group by stats_date, query_hash

{% endif %}
