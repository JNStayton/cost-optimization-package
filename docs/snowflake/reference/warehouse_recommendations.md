# Warehouse Recommendations — Snowflake

This document covers the warehouse optimization pipeline: identifying warehouses that need resizing, dbt models that are spilling during builds, and the most expensive dbt-authored queries by projected annual cost.

---

## Pipeline Overview

All three warehouse marts share intermediate models:

```
Staging                          Intermediate                         Marts
───────                          ────────────                         ─────

stg_snowflake__query_        ──► int_snowflake__query_          ─┐
  history                          history                       │
                                                                 ├──► int_snowflake__warehouse_query_stats_daily
stg_snowflake__sessions      ──► int_snowflake__dbt_sessions ───┤       │
                                                                 │       ├──► fct_snowflake__warehouse_config_recommendations
                                                                 │       │
                                                                 ├──► int_snowflake__warehouse_spillage_daily
                                                                 │       │
                                                                 │       ├──► fct_snowflake__warehouse_performance_recommendations
                                                                 │       │
                                                                 ├──► int_snowflake__warehouse_expensive_queries_daily
                                                                 │       │
stg_snowflake__warehouse_    ──► int_snowflake__warehouse_daily ─┤       ├──► fct_snowflake__expensive_query_recommendations
  metering_history                                               │
                                                                 │
int_snowflake__table_query_stats_daily ──────────────────────────┤
                                                                 │
int_dbt__relations ──────────────────────────────────────────────┘
```

### Shared Concept: dbt Session Scoping

All warehouse recommendations are scoped to **dbt-authored queries only** via `int_snowflake__dbt_sessions`. This model identifies Snowflake sessions where `client_environment:APPLICATION = 'dbt'`. Only queries from these sessions are evaluated — non-dbt workloads (BI tools, ad-hoc queries, applications) are excluded.

### Shared Concept: Enterprise vs Standard Edition

Controlled by `snowflake_enterprise_edition` variable (default: `true`).

| Feature | Enterprise+ | Standard |
|---------|------------|----------|
| Gen2 warehouses | Yes | Yes |
| Multi-cluster (MCW) | Yes | **No** — recommends "split workload" instead |
| Query Acceleration (QAS) | Yes | **No** |
| Credit attribution | `QUERY_ATTRIBUTION_HISTORY` (exact per-query credits) | Elapsed-time proration from `WAREHOUSE_METERING_HISTORY` (approximate) |
| Table-level spillage attribution | `ACCESS_HISTORY` (exact query→table mapping) | Not available — spillage mart produces no rows |
| Query-to-table attribution | `ACCESS_HISTORY` (exact) | `query_text ILIKE` matching (approximate, false-positive risk) |

### Warehouse Configuration Awareness

The sizing model and macro detect current warehouse configuration via `WAREHOUSE_EVENTS_HISTORY` (ACCOUNT_USAGE) and suppress redundant recommendations:

| Signal | Current Config | Edition | Recommendation |
|--------|---------------|---------|---------------|
| DML > threshold | Not Gen2 | Any | Enable Gen2 |
| DML > threshold | Already Gen2 | Any | Already optimized — review query patterns |
| High overload | Single-cluster | Enterprise+ | Enable multi-cluster |
| High overload | Single-cluster | Standard | Split workload across warehouses |
| High overload | Already multi-cluster | Enterprise+ | Review scaling policy / increase max_cluster_count |
| Moderate overload | Any | Any | Scale up single cluster |
| Low utilization | Any | Any | Scale down single cluster |
| Any | Adaptive | Any | Stable (self-optimizing) |

### Adaptive Warehouse Exclusion

Adaptive warehouses (`warehouse_size = 'ADAPTIVE'`) are filtered from the sizing pipeline. Sizing recommendations (scale up/down, Gen2, multi-cluster) do not apply to adaptive warehouses because Snowflake manages their resources automatically. Spillage and expensive query recommendations still apply to adaptive warehouses.

---

## Model 1: `fct_snowflake__warehouse_config_recommendations`

### Purpose

Evaluates warehouses running dbt workloads and recommends sizing actions based on concurrency signals, DML patterns, and idle credit waste.

### Decision Matrix

| Recommendation | Trigger | Rationale |
|---------------|---------|-----------|
| **Enable Gen2 warehouse** | `dml_ratio > 35%` (configurable) | Gen2 hardware is optimized for DML-heavy workloads (INSERT/UPDATE/DELETE/MERGE). DML-heavy warehouses see material performance improvements. |
| **Enable multi-cluster** | `median_overload > 5s` AND `overload/elapsed > 10%` | Severe concurrency bottleneck — queries queue for a significant portion of their total execution time. Multi-cluster (Enterprise+) or workload splitting is needed. |
| **Scale up single cluster** | `median_overload 1-5s` | Moderate concurrency bottleneck — a larger warehouse can process more concurrent queries without queuing. |
| **Scale down single cluster** | `median_execution < 5s` AND `median_overload < 0.5s` | Warehouse is oversized — queries are fast with no queuing, indicating more resources than needed. |
| **Stable** | None of the above | Metrics within healthy range. |

### Trend Analysis

Compares 7-day median overload to 30-day median overload:
- **Worsening** — 7-day > 30-day by more than 20%
- **Improving** — 7-day < 30-day by more than 20%
- **Stable** — within 20%

### Idle Credit Context

From `int_snowflake__warehouse_daily`, the model surfaces idle credit percentage (credits consumed while no queries are running). High idle credits alongside a "scale down" recommendation strengthens the case — scaling down also reduces idle spend.

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `warehouse_sizing_lookback_days` | `30` | Analysis window |
| `warehouse_sizing_dml_threshold` | `0.35` | DML ratio above which Gen2 is recommended |
| `warehouse_sizing_min_query_count` | `20` | Minimum dbt queries to evaluate a warehouse |

---

## Model 2: `fct_snowflake__warehouse_performance_recommendations`

### Purpose

Identifies dbt-managed tables whose build queries spill to local or remote storage, with warehouse-level trend context. The actionable unit is the **table** (which model's SQL is spilling), not the warehouse — fixing the query is almost always cheaper than scaling up.

### Recommendation Tiers

| Tier | Trigger | Severity |
|------|---------|----------|
| **Critical — Remote Spillage** | `total_gb_spilled_remote > 0.1` | Remote spill means compute exhausted both RAM and local SSD, falling back to S3. Dramatically increases elapsed time and credit consumption. |
| **Warn — Heavy Local Spillage** | `total_gb_spilled_local > 5` | Intermediate results exceed RAM. Significant performance degradation. |
| **Monitor — Moderate Spillage** | Above floor but not severe | Tolerable but worth profiling if the model is on a critical path. |

### Trend Analysis

The lookback window is split in half (`lookback_days / 2`). Recent-half spillage is compared to prior-half:
- **Worsening** — recent > prior by 20%+
- **Improving** — recent < prior by 20%+
- **Stable** — within 20%
- **Insufficient data** — lookback_days <= 2

### Enterprise+ Requirement

This model **requires Enterprise+ edition** (`snowflake_enterprise_edition = true`). Without ACCESS_HISTORY, table-level spillage attribution is not possible and the model produces zero rows with an explanatory message.

The `int_snowflake__warehouse_spillage_daily` intermediate (warehouse-level aggregation) works on all editions since it only joins query_history to dbt_sessions.

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `spillage_lookback_days` | `30` | Analysis window |
| `spillage_min_total_gb` | `0.05` | Minimum total spillage (GB) to appear in results |
| `spillage_min_runs` | `1` | Minimum DML/CTAS runs to appear in results |

---

## Model 3: `fct_snowflake__expensive_query_recommendations`

### Purpose

Surfaces the most expensive dbt-authored queries by projected annual cost, grouped by `query_hash` (so repeated runs of the same logical query are aggregated).

### Scoring

```
estimated_annual_cost_usd = (total_credits / lookback_days) * 365 * credit_rate_usd
```

Where `total_credits` comes from:
- **Primary**: `QUERY_ATTRIBUTION_HISTORY.credits_attributed_compute` — exact per-query credits (automatic, no configuration needed)
- **Fallback** (for short queries <= ~100ms where attribution is NULL): prorated from `WAREHOUSE_METERING_HISTORY` by each query's elapsed time share within the warehouse-hour bucket. Flagged via `credits_from_attribution = false`.

### Recommendation Tiers

| Tier | Trigger | Meaning |
|------|---------|---------|
| **High Cost** | `estimated_annual_cost_usd > $10,000` (configurable) | Review for refactor opportunities |
| **Tracked** | Above `min_total_credits` floor but below high threshold | Monitor — recurring credit consumption |

### Trend Analysis

Compares 7-day projected annual cost to 30-day projected annual cost:
- **Worsening** — 7-day > 30-day by 20%+
- **Improving** — 7-day < 30-day by 20%+
- **Stable** — within 20%

### Row Cap

This model is capped at `expensive_query_top_n` rows (default 50). The output contains at most that many rows, ordered by projected cost descending.

### dbt Node Attribution

The `dbt_node_id` column extracts the dbt model unique_id from the JSON comment dbt prepends to compiled queries (`/* {"app": "dbt", "node_id": "..."} */`). This allows attributing expensive queries back to specific dbt models.

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `expensive_query_lookback_days` | `30` | Analysis window |
| `credit_rate_usd` | `2` | Credit-to-dollar conversion rate |
| `expensive_query_high_cost_threshold` | `10000` | Annual cost threshold for "High Cost" tier |
| `expensive_query_min_total_credits` | `0.1` | Minimum credits to appear in results |
| `expensive_query_top_n` | `50` | Maximum rows in output |

---

## Sample Queries

### Warehouses that need attention

```sql
select
    warehouse_name,
    warehouse_size,
    recommendation,
    overload_trend,
    median_overload_sec_30d,
    dml_pct_30d,
    total_credits_30d,
    recommendation_reason
from <your_schema>.fct_snowflake__warehouse_config_recommendations
where recommendation != 'Stable — no sizing change recommended'
order by total_queries_30d desc;
```

### Tables causing remote spillage (critical)

```sql
select
    table_fqn,
    model_name,
    warehouse_name,
    total_gb_spilled_remote,
    total_gb_spilled_local,
    spill_trend,
    total_runs,
    recommendation_reason
from <your_schema>.fct_snowflake__warehouse_performance_recommendations
where recommendation like 'Critical%'
order by total_gb_spilled_remote desc;
```

### Most expensive dbt queries (annual projection)

```sql
select
    dbt_node_id,
    query_hash,
    warehouse_name,
    estimated_annual_cost_usd,
    cost_trend,
    total_runs_30d,
    avg_elapsed_sec,
    credits_from_attribution,
    recommendation_reason
from <your_schema>.fct_snowflake__expensive_query_recommendations
where recommendation like 'Review%'
order by estimated_annual_cost_usd desc;
```

### Credit waste from Standard-edition approximation

```sql
select
    dbt_node_id,
    estimated_annual_cost_usd,
    cost_trend,
    credits_from_attribution
from <your_schema>.fct_snowflake__expensive_query_recommendations
where not credits_from_attribution
order by estimated_annual_cost_usd desc;
```

---

## Notes

- **Adaptive warehouses:** Excluded from sizing recommendations. Spillage and expensive query models still apply to adaptive warehouses.
- **Credit approximation for short queries:** Queries <= ~100ms don't appear in QUERY_ATTRIBUTION_HISTORY. For these, credit estimates are prorated by elapsed time share across all queries in the warehouse-hour. This can over-estimate credits for short queries running alongside long ones on the same warehouse. The `credits_from_attribution` column flags approximate rows.
- **Lookback vs intermediate initial load:** If you set `expensive_query_lookback_days: 60` but the intermediate model only has 30 days of data (initial load window), the mart returns correct but incomplete results until the intermediate accumulates enough history.
- **ACCOUNT_USAGE latency:** Views can lag 45 minutes to 3 hours. Schedule dbt builds outside this window for complete results.
- **Median-of-medians approximation:** The sizing model computes `median(median_overload_ms)` across daily medians — an approximation of the true 30-day median. Acceptable for recommendation signals but not statistically exact.

---

## References

- [Snowflake Gen2 Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-gen2)
- [Adaptive Compute](https://docs.snowflake.com/en/user-guide/warehouses-adaptive) (excluded from sizing)
- [Multi-cluster Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster)
- [Query Acceleration Service](https://docs.snowflake.com/en/user-guide/query-acceleration-service)
- [QUERY_ATTRIBUTION_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_attribution_history)
- [Understanding Compute Cost](https://docs.snowflake.com/en/user-guide/cost-understanding-compute)
- [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
