{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ai_user_usage_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Daily AI credit consumption per user from CORTEX_AI_FUNCTIONS_USAGE_HISTORY.
  Grain: one row per (user_id, stats_date).
--#}

select
    md5(
        coalesce(to_varchar(cast(start_time as date)), '') || '|' ||
        coalesce(user_id, '')
    )                                                   as ai_user_usage_daily_key,
    cast(start_time as date)                            as stats_date,
    user_id,
    count(distinct query_id)                            as query_count,
    sum(credits)                                        as total_credits,
    count(distinct function_name)                       as distinct_functions_used,
    count(distinct model_name)                          as distinct_models_used,
    count(case when query_tag is not null and query_tag != '' then 1 end)
                                                        as tagged_query_count,
    count(case when query_tag is null or query_tag = '' then 1 end)
                                                        as untagged_query_count
from {{ ref('stg_snowflake__cortex_ai_functions_usage') }}
{% if is_incremental() %}
where cast(start_time as date) >= (
    select dateadd(day, -1, coalesce(max(stats_date), '1970-01-01'::date))
    from {{ this }}
)
{% endif %}
group by cast(start_time as date), user_id
