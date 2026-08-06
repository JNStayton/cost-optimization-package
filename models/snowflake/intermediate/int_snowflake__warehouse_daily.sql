{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='warehouse_daily_key',
    cluster_by=['stats_date'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily credit consumption per warehouse from WAREHOUSE_METERING_HISTORY.
  Separates execution credits from idle credits (total - compute).

  Grain: one row per (warehouse_name, stats_date).

  Idle credits signal: a warehouse with consistently high idle_credit_pct is
  a candidate for tighter auto-suspend settings. Paired with
  int_snowflake__warehouse_query_stats_daily, this gives the full picture of
  whether idle spend is offset by provisioning queue savings.
--#}

select
    md5(
        coalesce(to_varchar(cast(start_time as date)), '') || '|' ||
        coalesce(warehouse_name, '')
    )                                                           as warehouse_daily_key,
    cast(start_time as date)                                    as stats_date,
    warehouse_name,
    sum(credits_used)                                           as total_credits,
    sum(credits_used_compute)                                   as execution_credits,
    sum(credits_used - credits_used_compute)                    as idle_credits,
    round(
        sum(credits_used - credits_used_compute)
            / nullif(sum(credits_used), 0),
        4
    )                                                           as idle_credit_pct
from {{ ref('stg_snowflake__warehouse_metering_history') }}
{% if is_incremental() %}
where cast(start_time as date) >= (
    select dateadd(day, -1, coalesce(max(stats_date), '1970-01-01'::date))
    from {{ this }}
)
{% else %}
where start_time >= dateadd(day, -30, current_timestamp())
{% endif %}
group by cast(start_time as date), warehouse_name
