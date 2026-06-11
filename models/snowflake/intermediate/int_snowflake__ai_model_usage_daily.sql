{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ai_model_usage_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Daily AI usage by model and function, with token breakdowns.
  Grain: one row per (model_name, function_name, stats_date).

  Extracts input/output tokens from the METRICS array.
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

token_extraction as (
    select
        stats_date,
        function_name,
        model_name,
        query_id,
        user_id,
        credits,
        coalesce(
            (select sum(m.value:value::int)
             from lateral flatten(input => metrics) as m
             where m.value:key:metric::string = 'input'),
            0
        ) as input_tokens,
        coalesce(
            (select sum(m.value:value::int)
             from lateral flatten(input => metrics) as m
             where m.value:key:metric::string = 'output'),
            0
        ) as output_tokens,
        coalesce(
            (select sum(m.value:value::int)
             from lateral flatten(input => metrics) as m
             where m.value:key:metric::string = 'total'),
            0
        ) as total_tokens_flat
    from parsed
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
