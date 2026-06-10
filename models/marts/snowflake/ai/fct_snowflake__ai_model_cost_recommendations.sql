{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Model cost recommendations: identifies opportunities to downgrade models,
  trim prompts, or batch repeated calls.

  Grain: one row per (model_name, function_name) combination.

  Controlled by:
    - ai_spend_lookback_days                        (default 30)
    - ai_credit_rate_usd                            (default 2)
    - ai_model_downgrade_output_token_threshold     (default 100)
    - ai_prompt_bloat_input_token_threshold          (default 10000)
    - ai_batch_opportunity_min_daily_calls           (default 100)
    - ai_min_credits_for_recommendation             (default 1)
--#}

{% set lookback_days            = var('ai_spend_lookback_days', 30) %}
{% set credit_rate_usd          = var('ai_credit_rate_usd', 2) %}
{% set output_token_threshold   = var('ai_model_downgrade_output_token_threshold', 100) %}
{% set input_token_threshold    = var('ai_prompt_bloat_input_token_threshold', 10000) %}
{% set batch_min_daily          = var('ai_batch_opportunity_min_daily_calls', 100) %}
{% set min_credits              = var('ai_min_credits_for_recommendation', 1) %}

with model_stats as (
    select
        model_name,
        function_name,
        sum(total_credits)                                          as total_credits,
        sum(query_count)                                            as total_queries,
        sum(total_input_tokens)                                     as total_input_tokens,
        sum(total_output_tokens)                                    as total_output_tokens,
        sum(total_tokens)                                           as total_tokens,
        round(avg(avg_input_tokens_per_query), 0)                   as avg_input_tokens,
        round(avg(avg_output_tokens_per_query), 0)                  as avg_output_tokens,
        sum(unique_users)                                           as total_user_days,
        count(distinct stats_date)                                  as active_days,
        round(sum(query_count) * 1.0 / nullif(count(distinct stats_date), 0), 1)
                                                                    as avg_daily_calls
    from {{ ref('int_snowflake__ai_model_usage_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by model_name, function_name
    having sum(total_credits) >= {{ min_credits }}
),

scored as (
    select
        *,
        round(total_credits / {{ lookback_days }} * 365 * {{ credit_rate_usd }}, 2)
                                                                    as projected_annual_cost_usd,
        round(
            total_credits / nullif(total_output_tokens, 0) * 1000,
            6
        )                                                           as cost_per_1k_output_tokens,
        case
            when avg_output_tokens < {{ output_token_threshold }}
                 and total_credits > {{ min_credits }} * 5
                then 'model_downgrade'
            when avg_input_tokens > {{ input_token_threshold }}
                then 'prompt_bloat'
            when avg_daily_calls > {{ batch_min_daily }}
                then 'batch_opportunity'
            else 'monitor'
        end                                                         as recommendation_key
    from model_stats
)

select
    current_date()                                                  as snapshot_date,
    current_timestamp()                                             as analyzed_at,
    {{ lookback_days }}                                             as analysis_lookback_days,
    {{ credit_rate_usd }}                                           as credit_rate_usd,
    model_name,
    function_name,
    total_credits,
    total_queries,
    total_input_tokens,
    total_output_tokens,
    avg_input_tokens,
    avg_output_tokens,
    active_days,
    avg_daily_calls,
    projected_annual_cost_usd,
    cost_per_1k_output_tokens,
    case
        when recommendation_key = 'model_downgrade'
            then 'Consider cheaper model (low output complexity)'
        when recommendation_key = 'prompt_bloat'
            then 'Optimize prompts (high input token count)'
        when recommendation_key = 'batch_opportunity'
            then 'Evaluate caching or batching (high call frequency)'
        else 'Monitor — no immediate action'
    end                                                             as recommendation,
    case
        when recommendation_key = 'model_downgrade'
            then 'Average output of ' || avg_output_tokens || ' tokens/query suggests simple responses. '
                || 'Consider a smaller/cheaper model (e.g., llama3.1-8b, mistral-7b) for this '
                || function_name || ' workload. Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'prompt_bloat'
            then 'Average input of ' || avg_input_tokens || ' tokens/query (' || function_name || '). '
                || 'Trim system prompts, use RAG/Cortex Search for context injection, or reduce '
                || 'few-shot examples. Projected annual cost: $' || projected_annual_cost_usd || '.'
        when recommendation_key = 'batch_opportunity'
            then avg_daily_calls || ' calls/day to ' || model_name || ' (' || function_name || '). '
                || 'Evaluate application-level caching for repeated prompts or batch processing. '
                || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
        else 'Usage of ' || model_name || ' (' || function_name || ') within normal parameters. '
            || total_queries || ' queries over ' || active_days || ' days. '
            || 'Projected annual cost: $' || projected_annual_cost_usd || '.'
    end                                                             as recommendation_reason
from scored
order by
    case recommendation_key
        when 'model_downgrade'   then 1
        when 'prompt_bloat'      then 2
        when 'batch_opportunity' then 3
        else 4
    end,
    projected_annual_cost_usd desc
