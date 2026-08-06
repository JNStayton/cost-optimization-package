{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ai_spend_daily_key',
    cluster_by=['stats_date'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily AI credit consumption by service type from METERING_HISTORY.
  Grain: one row per (service_type, stats_date).
--#}

select
    md5(
        coalesce(to_varchar(cast(start_time as date)), '') || '|' ||
        coalesce(service_type, '')
    )                                                   as ai_spend_daily_key,
    cast(start_time as date)                            as stats_date,
    service_type,
    sum(credits_used)                                   as total_credits,
    sum(credits_used_compute)                           as compute_credits,
    sum(credits_used_cloud_services)                    as cloud_services_credits
from {{ ref('stg_snowflake__metering_history') }}
{% if is_incremental() %}
where cast(start_time as date) >= (
    select dateadd(day, -1, coalesce(max(stats_date), '1970-01-01'::date))
    from {{ this }}
)
{% endif %}
group by cast(start_time as date), service_type
