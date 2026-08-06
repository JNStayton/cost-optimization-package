{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ai_model_usage_daily_key',
    cluster_by=['stats_date'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily AI usage by model and function, with token breakdowns.
  Grain: one row per (model_name, function_name, stats_date).

  Token extraction uses lateral flatten on the METRICS array followed by
  conditional aggregation. The METRICS array structure is:
    [{"key":{"metric":"input","unit":"tokens"},"value":17},
     {"key":{"metric":"output","unit":"tokens"},"value":65}]
--#}

with parsed as (
    select
        cast(start_time as date)                        as stats_date,
        function_name,
        model_name,
        query_id,
        user_id,
        credits,
        metrics
    from {{ ref('stg_snowflake__cortex_ai_functions_usage') }}
    {% if is_incremental() %}
    where cast(start_time as date) >= (
        select dateadd(day, -1, coalesce(max(stats_date), '1970-01-01'::date))
        from {{ this }}
    )
    {% endif %}
),

flattened as (
    select
        p.stats_date,
        p.function_name,
        p.model_name,
        p.query_id,
        p.user_id,
        p.credits,
        f.value:key:metric::string                      as metric_name,
        f.value:value::int                              as metric_value
    from parsed as p,
    lateral flatten(input => p.metrics) as f
),

token_extraction as (
    select
        stats_date,
        function_name,
        model_name,
        query_id,
        user_id,
        credits,
        coalesce(sum(case when metric_name = 'input' then metric_value end), 0)  as input_tokens,
        coalesce(sum(case when metric_name = 'output' then metric_value end), 0) as output_tokens,
        coalesce(sum(case when metric_name = 'total' then metric_value end), 0)  as total_tokens_flat
    from flattened
    group by stats_date, function_name, model_name, query_id, user_id, credits
)

select
    md5(
        coalesce(to_varchar(stats_date), '') || '|' ||
        coalesce(function_name, '') || '|' ||
        coalesce(model_name, '')
    )                                                   as ai_model_usage_daily_key,
    stats_date,
    function_name,
    model_name,
    count(distinct query_id)                            as query_count,
    count(distinct user_id)                             as unique_users,
    sum(credits)                                        as total_credits,
    sum(input_tokens)                                   as total_input_tokens,
    sum(output_tokens)                                  as total_output_tokens,
    sum(
        case when total_tokens_flat > 0 then total_tokens_flat
             else input_tokens + output_tokens
        end
    )                                                   as total_tokens,
    round(avg(input_tokens), 0)                         as avg_input_tokens_per_query,
    round(avg(output_tokens), 0)                        as avg_output_tokens_per_query
from token_extraction
group by stats_date, function_name, model_name
