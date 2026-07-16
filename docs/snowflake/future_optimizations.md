# Future Optimizations — Snowflake

This document tracks Snowflake features that are in preview or recently GA'd that will require new optimization paths or modifications to existing ones. Each section outlines what to build once the feature is generally available and stable.

---

## Adaptive Compute (Preview — Enterprise+)

**Status:** Public Preview (as of Snowflake Summit 2026)  
**Availability:** Enterprise Edition, limited AWS regions (US West 2 Oregon, EU West 1 Ireland, AP Northeast 1 Tokyo)  
**Documentation:** [Adaptive Compute](https://docs.snowflake.com/en/user-guide/warehouses-adaptive)

### What It Is

Adaptive Compute is a new warehouse type (`WAREHOUSE_TYPE = 'ADAPTIVE'`) that eliminates manual sizing. Instead of configuring warehouse size, multi-cluster settings, QAS, and auto-suspend/resume, the system allocates resources per-query automatically.

Key properties:
- `MAX_QUERY_PERFORMANCE_LEVEL` — t-shirt size upper bound on per-query performance (XSMALL through X4LARGE, default XLARGE)
- `QUERY_THROUGHPUT_MULTIPLIER` — controls concurrency budget (integer, default 2; 0 = unlimited)

### How It Differs from Standard/Gen2

| Property | Standard/Gen2 | Adaptive |
|----------|--------------|----------|
| Size | User-configured (XSMALL–X4LARGE) | N/A — system decides per query |
| Multi-cluster | User-configured | N/A — automatic scaling |
| QAS | Separately enabled/configured | Built-in (no separate line item) |
| Auto-suspend/resume | User-configured | N/A — no suspend concept |
| Billing | Per-hour credits (by warehouse size) | Per-query credits (by resources consumed) |
| Detection | `warehouse_size` in QUERY_HISTORY = standard sizes | `warehouse_size = 'ADAPTIVE'` |

### Impact on Current Package

**Warehouse sizing recommendations (`fct_snowflake__warehouse_sizing_recommendations`):**
- Recommendations to "scale up," "scale down," "enable multi-cluster," or "enable Gen2" do not apply to adaptive warehouses
- Current fix: filter adaptive warehouses from the sizing intermediate (`warehouse_size != 'ADAPTIVE'`)
- Future: produce a separate recommendation category for adaptive warehouses

**Expensive query recommendations (`fct_snowflake__expensive_query_recommendations`):**
- Still relevant — expensive queries on adaptive warehouses still consume credits
- `QUERY_METERING_HISTORY` provides per-query credits natively for adaptive warehouses (more precise than elapsed-time proration)
- `warehouse_size = 'ADAPTIVE'` should be noted in output rather than displayed as a t-shirt size

**Spillage recommendations (`fct_snowflake__warehouse_spillage_recommendations`):**
- Fully relevant — spillage is a query-level phenomenon regardless of warehouse type
- No changes needed

### What to Build (Post-GA)

#### New Mart: `fct_snowflake__adaptive_warehouse_recommendations`

**Purpose:** Recommend tuning `MAX_QUERY_PERFORMANCE_LEVEL` and `QUERY_THROUGHPUT_MULTIPLIER` based on observed workload patterns.

**Data sources:**
- `QUERY_HISTORY` (where `warehouse_size = 'ADAPTIVE'`)
- `QUERY_METERING_HISTORY` (per-query credits for adaptive warehouses)
- `WAREHOUSE_LOAD_HISTORY` (queuing behavior for adaptive warehouses)

**Recommendation logic:**

| Signal | Detection | Recommendation |
|--------|-----------|----------------|
| Chronic queuing | High `queued_overload_time` despite adaptive | Increase `QUERY_THROUGHPUT_MULTIPLIER` |
| Underutilization | All queries using far less than `MAX_QUERY_PERFORMANCE_LEVEL` | Decrease `MAX_QUERY_PERFORMANCE_LEVEL` to reduce burst cost ceiling |
| Cost spikes without performance gain | High per-query credits but no improvement in elapsed time | Lower `MAX_QUERY_PERFORMANCE_LEVEL` — system is over-allocating |
| Conversion candidate | Standard warehouse with chronic overload + Enterprise+ + supported region | "Consider converting to Adaptive Compute" |

**Proposed intermediate:**
- `int_snowflake__adaptive_warehouse_daily` — daily stats per adaptive warehouse (queuing, per-query credit distribution, performance level utilization)

**Proposed variables:**
```yaml
vars:
  adaptive_warehouse_enabled: true
  adaptive_warehouse_lookback_days: 30
  adaptive_warehouse_queue_threshold_sec: 5
```

#### New Recommendation Tier in Sizing DAG

When adaptive warehouses are GA and broadly available, add a tier to the existing `fct_snowflake__warehouse_sizing_recommendations`:

```
Recommendation: "Convert to Adaptive Compute"
Conditions:
  - Account is Enterprise Edition or higher
  - Warehouse has chronic overload (median_overload_sec > 5s for 14+ days)
  - Manual tuning hasn't resolved the issue (multiple size/MCW changes in history)
  - Region supports adaptive warehouses
```

### Detection Query (for testing)

```sql
-- Find adaptive warehouses in the account
SELECT DISTINCT warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_size = 'ADAPTIVE'
  AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP());

-- Or via SHOW WAREHOUSES
SHOW WAREHOUSES;
-- Look for warehouse_type = 'ADAPTIVE' in results
```

---

## Gen2 Default Behavior Change (BCR-2250)

**Status:** Pending (rolling out by region)  
**Impact:** New organizations after June/July 2025 default to Gen2 for standard warehouses  
**Documentation:** [Gen2 Standard Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-gen2)

### Impact on Current Package

The "Enable Gen2" recommendation in `fct_snowflake__warehouse_sizing_recommendations` remains valid for:
- Existing Gen1 warehouses that haven't been converted
- Accounts in organizations created before the default changed

**No code changes needed** — the recommendation correctly identifies DML-heavy warehouses that would benefit from Gen2's optimized DML execution path. As adoption of Gen2 increases organically, this recommendation will fire less often.

### Detection

Gen2 can be detected via `SHOW WAREHOUSES` (the `resource_constraint` column shows `STANDARD_GEN_2`). This is NOT currently reflected in `ACCOUNT_USAGE` views — only in `SHOW WAREHOUSES` output. A future enhancement could query `SHOW WAREHOUSES` and exclude already-Gen2 warehouses from the recommendation.

---

## Interactive Warehouses

**Status:** Generally Available  
**Documentation:** [Interactive Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-interactive)

### Relevance to This Package

Interactive warehouses are designed for low-latency, high-concurrency dashboard and application workloads. They use a different billing model and are not a cost optimization target in the same way — they're a specialized product for a specific use case.

**No optimization path planned** unless customer demand emerges. If added, it would focus on:
- Identifying workloads that are paying standard warehouse rates but have interactive-warehouse characteristics (many short queries, low latency requirements)
- Recommending conversion to interactive when the workload profile matches

---

## References

- [Adaptive Compute documentation](https://docs.snowflake.com/en/user-guide/warehouses-adaptive)
- [Gen2 Standard Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-gen2)
- [Interactive Warehouses](https://docs.snowflake.com/en/user-guide/warehouses-interactive)
- [CREATE ADAPTIVE WAREHOUSE](https://docs.snowflake.com/en/sql-reference/sql/create-adaptive-warehouse)
- [QUERY_METERING_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_metering_history)
- [WAREHOUSE_LOAD_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/warehouse_load_history)
- [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
