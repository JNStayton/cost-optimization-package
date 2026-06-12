{{
  config(
    materialized='table',
  )
}}

{#--
  Token efficiency recommendations: identifies prompt bloat, repeated queries,
  runaway consumption, and model-task mismatches at the query pattern level.

  Grain: one row per (model_name, function_name, query_tag) grouping — or per
  model/function if query_tag is unavailable.

  Controlled by:
    - ai_spend_lookback_days                    (default 30)
    - ai_credit_rate_usd                        (default 2)
    - ai_prompt_bloat_input_token_threshold     (default 10000)
    - ai_batch_opportunity_min_daily_calls      (default 100)
    - ai_min_queries_for_recommendation         (default 10)
--#}

{% set lookback_days            = var('ai_spend_lookback_days', 30) %}
{% set credit_rate_usd          = var('ai_credit_rate_usd', 2) %}
{% set input_token_threshold    = var('ai_prompt_bloat_input_token_threshold', 10000) %}
{% set batch_min_daily          = var('ai_batch_opportunity_min_daily_calls', 100) %}
{% set min_queries              = var('ai_min_queries_for_recommendation', 10) %}

with raw_usage as (
    select
        cast(start_time as date)                                    as stats_date,
        function_name,
        model_name,
        query_id,
        query_tag,
        user_id,
        credits,
        is_completed,
        metrics
    from {{ ref('stg_snowflake__cortex_ai_functions_usage') }}
    where cast(start_time as date) >= dateadd(day, -{{ lookback_days }}, current_date())
),

flattened as (
    select
        r.stats_date,
        r.function_name,
        r.model_name,
        r.query_id,
        r.query_tag,
        r.user_id,
        r.credits,
        r.is_completed,
        f.value:key:metric::string                                  as metric_name,
        f.value:value::int                                          as metric_value
    from raw_usage as r,
    lateral flatten(input => r.metrics) as f
),

usage_base as (
    select
        stats_date,
        function_name,
        model_name,
        query_id,
        query_tag,
        user_id,
        credits,
        is_completed,
        coalesce(sum(case when metric_name = 'input' then metric_value end), 0)  as input_tokens,
        coalesce(sum(case when metric_name = 'output' then metric_value end), 0) as output_tokens
    from flattened
    group by stats_date, function_name, model_name, query_id, query_tag, user_id, credits, is_completed
),

pattern_stats as (
    select
        model_name,
        function_name,
        coalesce(nullif(query_tag, ''), 'untagged')                 as query_pattern,
        count(distinct query_id)                                    as total_queries,
        count(distinct stats_date)                                  as active_days,
        round(count(distinct query_id) * 1.0 / nullif(count(distinct stats_date), 0), 1)
                                                                    as avg_daily_calls,
        sum(credits)                                                as total_credits,
        round(avg(input_tokens), 0)                                 as avg_input_tokens,
        round(avg(output_tokens), 0)                                as avg_output_tokens,
        round(
            avg(input_tokens) * 1.0 / nullif(avg(output_tokens), 0),
            1
        )                                                           as input_output_ratio,
        count(distinct user_id)                                     as unique_users,
        count(case when is_completed = false then 1 end)            as incomplete_count,
        round(
            count(case when is_completed = false then 1 end) * 100.0
                / nullif(count(*), 0),
            1
        )                                                           as incomplete_pct
    from usage_base
    group by model_name, function_name, coalesce(nullif(query_tag, ''), 'untagged')
    having count(distinct query_id) >= {{ min_queries }}
),

scored as (
    select
        *,
        round(total_credits / {{ lookback_days }} * 365 * {{ credit_rate_usd }}, 2)
                                                                    as projected_annual_cost_usd,
        case
            when incomplete_pct > 20 and total_credits > 5
                then 'high_failure_rate'
            when avg_input_tokens > {{ input_token_threshold }}
                then 'prompt_bloat'
            when avg_daily_calls > {{ batch_min_daily }}
                 and input_output_ratio > 5
                then 'cache_candidate'
            when input_output_ratio > 50
                then 'extreme_ratio'
            else 'efficient'
        end                                                         as recommendation_key
    from pattern_stats
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    model_name,
    function_name,
    query_pattern,
    total_queries,
    active_days,
    avg_daily_calls,
    total_credits,
    projected_annual_cost_usd,
    avg_input_tokens,
    avg_output_tokens,
    input_output_ratio,
    unique_users,
    incomplete_count,
    incomplete_pct,
    case
        when recommendation_key = 'high_failure_rate'
            then 'Investigate high failure/cancellation rate'
        when recommendation_key = 'prompt_bloat'
            then 'Reduce input tokens (prompt optimization)'
        when recommendation_key = 'cache_candidate'
            then 'Implement caching for repeated high-frequency calls'
        when recommendation_key = 'extreme_ratio'
            then 'Extreme input:output ratio — context likely oversized'
        else 'Efficient — no action needed'
    end                                                             as recommendation,
    case
        when recommendation_key = 'high_failure_rate'
            then incomplete_pct || '% of queries incomplete/cancelled ('
                || incomplete_count || ' of ' || total_queries || '). '
                || 'Wasted token spend on failed completions. '
                || 'Check STATEMENT_TIMEOUT_IN_SECONDS settings or prompt quality. '
                || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'prompt_bloat'
            then 'Average ' || avg_input_tokens || ' input tokens for '
                || avg_output_tokens || ' output tokens (' || function_name || ' / '
                || model_name || '). '
                || 'Consider: trim system prompts, use Cortex Search for context retrieval, '
                || 'reduce few-shot examples. '
                || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'cache_candidate'
            then avg_daily_calls || ' calls/day with ' || coalesce(input_output_ratio, 0)
                || ':1 input:output ratio. '
                || 'High-frequency calls with heavy context suggest cacheable responses. '
                || 'Implement application-level response caching. '
                || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'extreme_ratio'
            then 'Input:output ratio of ' || coalesce(input_output_ratio, 0) || ':1 ('
                || avg_input_tokens || ' in / ' || coalesce(avg_output_tokens, 0) || ' out). '
                || 'Sending far more context than needed for the output. '
                || 'Evaluate whether full context is necessary or if summarization/RAG would suffice. '
                || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
        else coalesce(model_name, 'unknown') || ' / ' || coalesce(function_name, 'unknown') || ' (' || query_pattern || ') — '
            || 'efficient usage. ' || total_queries || ' queries over ' || active_days || ' days.'
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'high_failure_rate' then 1
        when 'prompt_bloat'      then 2
        when 'cache_candidate'   then 3
        when 'extreme_ratio'     then 4
        else 5
    end,
    projected_annual_cost_usd desc
