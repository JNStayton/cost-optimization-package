# AI/Cortex Spend Optimization — Design Document

**Status:** Draft  
**Last updated:** 2026-06-09  
**Author:** Jessica Stayton

---

## Overview

This optimization path tracks and monitors Snowflake Cortex AI consumption (tokens, credits, and dollar spend) and produces actionable recommendations for reducing cost without degrading AI workload quality.

---

## Data Sources

### High-Level Metering (credits billed)

Credits appear in `SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY` under 5 AI-specific service types:

| Service Type | Covers |
|---|---|
| `AI_SERVICES` | Cortex LLM functions (AI_COMPLETE, EXTRACT, etc.), Analyst, Search, Document AI, Fine-tuning |
| `CORTEX_AGENTS` | Cortex Agents API and Orchestrator |
| `CORTEX_CODE_CLI` | Cortex Code CLI |
| `CORTEX_CODE_SNOWSIGHT` | Cortex Code in Snowsight |
| `SNOWFLAKE_INTELLIGENCE` | Snowflake Intelligence features |

Additionally, `AI_INFERENCE` (Cortex REST API) is billed in **dollars** (not credits) and does NOT appear in METERING_DAILY. It has its own view: `CORTEX_REST_API_USAGE_HISTORY`.

### Granular Usage Views (per-query/per-model detail)

| Canonical View | Grain | Key Columns | Maps To |
|---|---|---|---|
| `CORTEX_AISQL_USAGE_HISTORY` | Per query x model x function | query_id, function_name, model_name, user_id, tokens (input/output), token_credits, warehouse_id, query_tag | AI_SERVICES |
| `CORTEX_AGENT_USAGE_HISTORY` | Per request (hierarchical) | agent name, schema, token_credits, parent_request_id | CORTEX_AGENTS |
| `SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY` | Per request (hierarchical) | token_credits, AI function credits | SNOWFLAKE_INTELLIGENCE |
| `CORTEX_ANALYST_USAGE_HISTORY` | Hourly by user | credits, request_count | AI_SERVICES |
| `CORTEX_SEARCH_SERVING_USAGE_HISTORY` | Hourly | credits (serving) | AI_SERVICES |
| `CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY` | Batch tokens + storage | embed tokens | AI_SERVICES |
| `CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY` | Per doc processing job | credits_used, pages | AI_SERVICES |
| `CORTEX_FINE_TUNING_USAGE_HISTORY` | Hourly | token_credits | AI_SERVICES |
| `CORTEX_CODE_CLI_USAGE_HISTORY` | Per usage | token_credits | CORTEX_CODE_CLI |
| `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` | Per usage | token_credits | CORTEX_CODE_SNOWSIGHT |
| `CORTEX_REST_API_USAGE_HISTORY` | Per request | tokens (input/output), model | Dollar-billed (not in METERING) |

### Important: View Overlap Rules

- `CORTEX_FUNCTIONS_USAGE_HISTORY` (deprecated) -> `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` (intermediate) -> `CORTEX_AISQL_USAGE_HISTORY` (current canonical). **Do not sum these together.**
- `DOCUMENT_AI_USAGE_HISTORY` is a subset of `CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY`. **Do not sum both.**
- The sum of all granular views will NOT exactly match `METERING_HISTORY` for `AI_SERVICES` — this is expected (subset exposure). Use granular views for relative attribution, not exact reconciliation.

### Latency

| View | Max Latency |
|---|---|
| `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` | 60 minutes |
| `CORTEX_REST_API_USAGE_HISTORY` | 45 minutes |
| `CORTEX_AGENT_USAGE_HISTORY` | ~3 hours |
| `METERING_HISTORY` | ~3 hours |
| `METERING_DAILY_HISTORY` (org-level) | ~2 hours |

---

## Proposed DAG Structure

```
models/
  staging/snowflake/
    stg_snowflake__metering_history.sql              -- filtered to AI service types only
    stg_snowflake__cortex_aisql_usage.sql            -- canonical per-query LLM usage
    stg_snowflake__cortex_agent_usage.sql            -- Cortex Agent usage
    stg_snowflake__cortex_search_usage.sql           -- Search serving credits
    stg_snowflake__cortex_rest_api_usage.sql         -- REST API (dollar-billed, separate)

  intermediate/snowflake/
    int_snowflake__ai_spend_daily.sql                -- daily credits by service type
    int_snowflake__ai_model_usage_daily.sql          -- daily by model x function (tokens + credits)
    int_snowflake__ai_user_usage_daily.sql           -- daily by user (top consumers)
    int_snowflake__ai_agent_usage_daily.sql          -- daily by Cortex Agent name

  marts/snowflake/ai/
    fct_snowflake__ai_spend_overview.sql             -- total AI spend summary + trend
    fct_snowflake__ai_model_cost_recommendations.sql -- model selection optimization
    fct_snowflake__ai_user_spend_recommendations.sql -- per-user budget recommendations
    fct_snowflake__ai_token_efficiency_recommendations.sql -- prompt/response optimization
```

---

## Mart Model Specifications

### `fct_snowflake__ai_spend_overview`

**Purpose:** Executive-level summary of AI spend across all service types with trend analysis.

**Grain:** One row per (service_type, stats_date) or one row per service_type (summary).

**Key outputs:**
- Total AI credits by service type (30-day window)
- Week-over-week and month-over-month growth rates
- Projected monthly/annual cost (using configurable credit_rate_usd)
- Percentage breakdown by service type

---

### `fct_snowflake__ai_model_cost_recommendations`

**Purpose:** Identify opportunities to use cheaper models or optimize prompt design.

**Grain:** One row per (model_name, function_name) combination with recommendations.

**Recommendation logic:**

| Signal | Detection | Recommendation |
|---|---|---|
| Expensive model for simple output | avg output tokens < `ai_model_downgrade_output_token_threshold` (default 100) | "Downgrade to cheaper model (e.g., llama3.1-8b or mistral-7b)" |
| High input:output ratio | avg input tokens > 10x avg output tokens | "Prompt likely over-contextualized — trim system prompts" |
| Batch opportunity | Same model+function called 100+ times/day with low variance in input size | "Consider batching or caching responses" |
| Cost-inefficient model for task | Model X costs 10x more per token than Model Y for same function, similar output quality proxy | "Evaluate switching from Model X to Model Y" |

**Key metrics per model:**
- total_credits_30d
- total_tokens_input / total_tokens_output
- avg_tokens_per_query
- cost_per_1k_output_tokens (derived from credits / output tokens)
- unique_users, query_count

---

### `fct_snowflake__ai_user_spend_recommendations`

**Purpose:** Identify top consumers, detect spikes, and suggest per-user budget thresholds.

**Grain:** One row per user.

**Recommendation logic:**

| Signal | Detection | Recommendation |
|---|---|---|
| Sudden spike | 7-day spend > `ai_user_spike_threshold_pct` (default 200%) of 30-day daily average | "User X spending 3x their baseline — investigate" |
| Concentration risk | Single user > 50% of total AI spend | "Heavy concentration — consider distributing workload or setting budget caps" |
| No attribution | User's queries lack query_tag | "Governance gap — set QUERY_TAG for cost attribution by project/team" |
| Budget suggestion | Based on historical p90 usage | "Suggested monthly budget: X credits (based on p90 of last 90 days)" |

---

### `fct_snowflake__ai_token_efficiency_recommendations`

**Purpose:** Surface token-level inefficiencies — prompt bloat, wasted completions, repeated queries.

**Grain:** One row per (query_hash or query_tag grouping) with recommendations.

**Recommendation logic:**

| Signal | Detection | Recommendation |
|---|---|---|
| Prompt bloat | Input tokens > `ai_prompt_bloat_input_token_threshold` (default 10000) | "Queries averaging 15K input tokens — trim context or use RAG/search instead" |
| Repeated identical queries | Same query_tag + model, 50+ identical calls/day | "Caching opportunity — implement application-level caching for repeated prompts" |
| Low efficiency ratio | Credits consumed but IS_COMPLETED = false on many rows | "High cancellation/failure rate — investigate prompt quality or timeout settings" |
| Runaway queries | Single query consuming > X credits (across hourly windows) | "Potential runaway — consider STATEMENT_TIMEOUT_IN_SECONDS or per-query credit limits" |

---

## Configurable Variables

```yaml
# dbt_project.yml
vars:
  # Lookback windows
  ai_spend_lookback_days: 30
  ai_spend_initial_lookback_days: 30

  # Model recommendation thresholds
  ai_model_downgrade_output_token_threshold: 100
  ai_prompt_bloat_input_token_threshold: 10000
  ai_batch_opportunity_min_daily_calls: 100

  # User recommendation thresholds
  ai_user_spike_threshold_pct: 200
  ai_user_concentration_threshold_pct: 50

  # Cost projection
  ai_credit_rate_usd: 2

  # Minimum activity for recommendations
  ai_min_credits_for_recommendation: 1
  ai_min_queries_for_recommendation: 10

  # REST API (dollar-billed) — separate tracking
  ai_rest_api_enabled: true
```

---

## Key Optimization Levers (Summary)

| Lever | Source Signal | Expected Impact |
|---|---|---|
| Model downgrade | High-cost model with simple outputs | 5-10x cost reduction per query |
| Prompt trimming | High input:output ratio | 2-5x reduction in input token cost |
| Caching | Repeated identical prompts | Eliminates redundant token spend entirely |
| Agent loop limits | Long parent_request_id chains | Prevents runaway credit consumption |
| Search right-sizing | Serving credits vs actual query volume | Reduce idle search service cost |
| User budgets | Concentration risk | Governance + predictability |
| REST API visibility | Dollar-billed spend invisible in credit dashboards | Surface hidden costs |

---

## Edition Requirements

| Feature | Standard | Enterprise+ |
|---|---|---|
| `METERING_HISTORY` (AI service types) | Yes | Yes |
| `CORTEX_AISQL_USAGE_HISTORY` | Yes | Yes |
| `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` | Yes | Yes |
| `CORTEX_AGENT_USAGE_HISTORY` | Yes | Yes |
| `CORTEX_REST_API_USAGE_HISTORY` | Yes | Yes |
| `ORGANIZATION_USAGE` views (cross-account) | No | Yes |

All AI usage views are available on Standard edition. No Enterprise+ gating required for this optimization path.

---

## Open Questions

1. **Scope:** Should we also include Cortex Search index build costs (serverless compute), or just serving/query costs?
2. **REST API handling:** Since REST API is dollar-billed (not credits), should it be a separate mart or integrated into the overview with a conversion factor?
3. **Agent recursion depth:** Should we detect and flag deeply-nested agent calls (many parent_request_id hops) as a cost risk?
4. **Fine-tuning:** Include fine-tuning cost tracking, or defer (likely low volume for most users)?
5. **Cortex Code tracking:** Some users may not want to surface their own CoCo/Cortex Code usage in recommendations. Should this be opt-in via a variable?

---

## References

### Primary Guides
- [Managing Cortex AI Function costs with Account Usage](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-func-cost-management)
- [Snowflake Service Consumption Table (token pricing per model)](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
- [Cortex REST API overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api)

### Account Usage View References
- [CORTEX_AISQL_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_aisql_usage_history) — canonical per-query LLM token/credit usage
- [CORTEX_AI_FUNCTIONS_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_ai_functions_usage_history) — per-query function usage (hourly windows)
- [CORTEX_AGENT_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_agent_usage_history) — Cortex Agents billing
- [CORTEX_ANALYST_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_analyst_usage_history) — Cortex Analyst usage
- [CORTEX_SEARCH_SERVING_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_search_serving_usage_history) — Search serving credits
- [CORTEX_SEARCH_DAILY_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_search_daily_usage_history) — Search daily aggregated
- [CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_search_batch_query_usage_history) — Batch embed tokens
- [CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_document_processing_usage_history) — Document AI / AI_PARSE_DOCUMENT
- [CORTEX_FINE_TUNING_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_fine_tuning_usage_history) — Fine-tuning token credits
- [CORTEX_CODE_CLI_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_code_cli_usage_history) — Cortex Code CLI usage
- [CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_code_snowsight_usage_history) — Cortex Code in Snowsight usage
- [CORTEX_REST_API_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/cortex_rest_api_usage_history) — REST API (dollar-billed)
- [SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/snowflake_intelligence_usage_history) — Snowflake Intelligence
- [METERING_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/metering_history) — Account-level credit metering
- [METERING_DAILY_HISTORY (Organization Usage)](https://docs.snowflake.com/en/sql-reference/organization-usage/metering_daily_history) — Org-level daily credits

### Community Resources
- [How to track and understand Cortex AI-related charges](https://community.snowflake.com/s/article/how-to-track-and-understand-cortex-ai-related-charges) — comprehensive FAQ on the "Big Five" AI service types and view lineage

### Related Snowflake Functions
- [AI_COUNT_TOKENS](https://docs.snowflake.com/en/sql-reference/functions/ai_count_tokens) — estimate token count before calling LLM functions
- [SYSTEM$CANCEL_QUERY](https://docs.snowflake.com/en/sql-reference/functions/system_cancel_query) — cancel runaway queries programmatically
