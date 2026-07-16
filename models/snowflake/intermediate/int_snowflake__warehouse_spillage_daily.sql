{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='warehouse_spillage_daily_key',
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily spillage aggregated at the warehouse level, scoped to dbt sessions.
  Provides the trend context for fct_snowflake__warehouse_spillage_recommendations
  (e.g., "this warehouse has been spilling for 14 of the last 30 days").

  Grain: one row per (warehouse_name, stats_date).

  Note: table-level spillage detail lives in int_snowflake__table_query_stats_daily.
  This model is warehouse-scoped only — the mart recommendation is still at table
  grain (which table is spilling), but uses this model to add warehouse-level
  trend context to the recommendation_reason.

  This model does NOT require ACCESS_HISTORY — it aggregates spillage from
  query_history scoped to dbt sessions. ACCESS_HISTORY is only required for
  table-level attribution (in int_snowflake__table_query_stats_daily).
--#}

with dbt_spilling_queries as (
    select
        qh.query_id,
        cast(qh.query_start_time as date)           as stats_date,
        qh.warehouse_name,
        qh.bytes_spilled_local,
        qh.bytes_spilled_remote,
        qh.total_elapsed_time_ms
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_snowflake__dbt_sessions') }} as s
        on qh.session_id = s.session_id
    where
        qh.warehouse_name is not null
        and (qh.bytes_spilled_local > 0 or qh.bytes_spilled_remote > 0)
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
    )                                                               as warehouse_spillage_daily_key,
    stats_date,
    warehouse_name,
    count(distinct query_id)                                        as spilling_query_count,
    round(sum(bytes_spilled_local) / power(1024, 3), 4)            as total_gb_spilled_local,
    round(sum(bytes_spilled_remote) / power(1024, 3), 4)           as total_gb_spilled_remote,
    round(avg(bytes_spilled_local) / power(1024, 3), 4)            as avg_gb_spilled_local,
    round(avg(bytes_spilled_remote) / power(1024, 3), 4)           as avg_gb_spilled_remote,
    round(avg(total_elapsed_time_ms) / 1000.0, 2)                  as avg_elapsed_sec
from dbt_spilling_queries
group by stats_date, warehouse_name
