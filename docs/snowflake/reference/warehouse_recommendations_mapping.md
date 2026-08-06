# Warehouse Recommendations Mapping

Complete symptom-to-optimization map for warehouse-level recommendations. Organized by symptom priority (most costly first), with all config branches, edition constraints, and flags for what's not currently measured.

---

## Signal Availability

| Signal | Currently Measured? | Source |
|--------|:---:|--------|
| `idle_credit_pct` | Yes | INT_SNOWFLAKE__WAREHOUSE_DAILY |
| `median_overload_ms` / `overload_to_elapsed_ratio` | Yes | INT_SNOWFLAKE__WAREHOUSE_QUERY_STATS_DAILY |
| `median_provisioning_ms` | Yes | INT_SNOWFLAKE__WAREHOUSE_QUERY_STATS_DAILY |
| `total_gb_spilled_local` / `remote` | Yes | INT_SNOWFLAKE__WAREHOUSE_SPILLAGE_DAILY |
| `avg_query_load_pct` | Yes | INT_SNOWFLAKE__WAREHOUSE_QUERY_STATS_DAILY |
| `is_multicluster` | Yes | INT_SNOWFLAKE__WAREHOUSE_CONFIG |
| `warehouse_size` | Yes | INT_SNOWFLAKE__WAREHOUSE_CONFIG |
| `is_gen2` | Yes | INT_SNOWFLAKE__WAREHOUSE_CONFIG |
| `auto_suspend` | No | Coming via SHOW WAREHOUSES macro |
| `auto_resume` | No | Coming via SHOW WAREHOUSES macro |
| `scaling_policy` | No | Coming via SHOW WAREHOUSES macro |
| `max_cluster_count` | No | Coming via SHOW WAREHOUSES macro |
| `min_cluster_count` | No | Coming via SHOW WAREHOUSES macro |
| `snowflake_edition` | No | Need org account view; we currently have a variable a user can set for this value and a default fallback, and without org admin permissions we cannot access this programmatically from Snowflake |
| `queued_overload_time` (per-query) | No | Available in QUERY_HISTORY but not aggregated |
| `queued_provisioning_time` (per-query) | No | Available in QUERY_HISTORY but not aggregated |
| `avg_queued_load` | No | In WAREHOUSE_LOAD_HISTORY, not staged |
| `avg_running` | No | In WAREHOUSE_LOAD_HISTORY, not staged |

---

## 1. HIGH IDLE CREDITS (Priority: High -- direct cost waste)

**Signal:** `idle_credit_pct > 0.20` (30-day window)

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 1.1 | `auto_suspend > 60` | Any | `ALTER WAREHOUSE {wh} SET AUTO_SUSPEND = 60;` | Warehouse is idle {idle_credit_pct}% of the time. Current auto_suspend is {auto_suspend}s -- reducing to 60s will eliminate ~{estimated_savings} idle credits/month without impacting query performance for most workloads. |
| 1.2 | `auto_suspend <= 60 AND is_multicluster = TRUE AND scaling_policy = 'ECONOMY'` | Enterprise+ | `ALTER WAREHOUSE {wh} SET SCALING_POLICY = 'STANDARD';` | Warehouse is idle {idle_credit_pct}% despite auto_suspend={auto_suspend}s. ECONOMY scaling policy keeps clusters running for 2-3 extra minutes after load drops. STANDARD scaling shuts down idle clusters immediately. |
| 1.3 | `auto_suspend <= 60 AND is_multicluster = TRUE AND scaling_policy = 'STANDARD' AND max_cluster_count > 2` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MAX_CLUSTER_COUNT = {max_cluster_count - 1};` | Warehouse is idle {idle_credit_pct}% with aggressive suspend and STANDARD scaling. Reducing max clusters from {max_cluster_count} to {max_cluster_count - 1} limits over-provisioning while still allowing scale-out. |
| 1.4 | `auto_suspend <= 60 AND is_multicluster = TRUE AND scaling_policy = 'STANDARD' AND max_cluster_count <= 2 AND min_cluster_count > 1` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MIN_CLUSTER_COUNT = 1;` | Warehouse is idle {idle_credit_pct}% but min_cluster_count={min_cluster_count} forces clusters to stay running. Setting min to 1 allows full scale-down during low-demand periods. |
| 1.5 | `auto_suspend <= 60 AND is_multicluster = FALSE AND edition = 'STANDARD'` | Standard | No DDL -- operational recommendation | Warehouse is idle {idle_credit_pct}% with optimal suspend settings. Consolidate workloads onto fewer warehouses or schedule batch jobs into tighter windows to reduce total active time. MCW is not available on Standard edition. |
| 1.6 | `auto_suspend <= 60 AND is_multicluster = FALSE AND edition != 'STANDARD' AND avg_query_load_pct > 80` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = 'STANDARD';` | Warehouse is idle {idle_credit_pct}% but also hitting high load ({avg_query_load_pct}%). This pattern suggests bursty workloads -- MCW with STANDARD scaling will handle bursts and scale to zero between them. |
| 1.7 | `auto_suspend <= 60 AND is_multicluster = FALSE AND avg_query_load_pct < 50` | Any | No DDL -- operational recommendation | Warehouse is idle {idle_credit_pct}% and underloaded when active ({avg_query_load_pct}% avg load). Consider consolidating this warehouse's workloads into another warehouse to eliminate the idle overhead entirely. |

---

## 2. OVERLOAD QUEUING (Priority: High -- performance degradation + potential cost if queries are long)

**Signal:** `overload_to_elapsed_ratio > 0.10 OR median_overload_ms > 500`

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 2.1 | `is_multicluster = FALSE AND edition != 'STANDARD'` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = 'STANDARD';` | Queries are spending {overload_to_elapsed_ratio*100}% of elapsed time queued behind other queries. Enabling multi-cluster allows concurrent workloads to scale out rather than queue. |
| 2.2 | `is_multicluster = FALSE AND edition = 'STANDARD' AND size is not max` | Standard | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_up}';` | Queries are spending {overload_to_elapsed_ratio*100}% of elapsed time queued. MCW is not available on Standard edition. Scaling up from {current_size} to {next_size_up} doubles compute capacity to reduce concurrency pressure. |
| 2.3 | `is_multicluster = FALSE AND edition = 'STANDARD' AND size is max` | Standard | No DDL -- operational recommendation | Queries are spending {overload_to_elapsed_ratio*100}% of elapsed time queued. Warehouse is already at maximum size and MCW is unavailable on Standard edition. Options: upgrade edition, split workloads across multiple warehouses, or stagger scheduled jobs. |
| 2.4 | `is_multicluster = TRUE AND max_cluster_count < 10 AND scaling_policy = 'ECONOMY'` | Enterprise+ | `ALTER WAREHOUSE {wh} SET SCALING_POLICY = 'STANDARD';` | Queries are queuing ({overload_to_elapsed_ratio*100}% overload ratio) despite MCW being enabled. ECONOMY scaling waits 2-3 minutes before adding clusters -- switching to STANDARD will spin up new clusters immediately when load increases. |
| 2.5 | `is_multicluster = TRUE AND max_cluster_count < 10 AND scaling_policy = 'STANDARD'` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MAX_CLUSTER_COUNT = {max_cluster_count + 1};` | Queries are queuing ({overload_to_elapsed_ratio*100}% overload ratio) with STANDARD scaling at max_cluster_count={max_cluster_count}. The warehouse is hitting its cluster ceiling -- increasing by 1 provides additional burst capacity. |
| 2.6 | `is_multicluster = TRUE AND max_cluster_count >= 10` | Enterprise+ | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_up}';` | Queries are queuing ({overload_to_elapsed_ratio*100}% overload ratio) with MCW at {max_cluster_count} clusters. Cluster count is already high -- scaling up warehouse size will give each cluster more compute capacity, reducing per-query execution time and freeing concurrency slots faster. |

---

## 3. PROVISIONING QUEUE (Priority: Medium -- latency impact, especially for interactive users)

**Signal:** `median_provisioning_ms > 2000` (2s+)

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 3.1 | `auto_suspend < 60 AND auto_resume = TRUE` | Any | `ALTER WAREHOUSE {wh} SET AUTO_SUSPEND = 60;` | Median provisioning wait is {median_provisioning_ms}ms -- queries are waiting for the warehouse to cold-start. Current auto_suspend={auto_suspend}s is aggressive, causing frequent suspend/resume cycles. Increasing to 60s reduces cold starts while keeping idle costs minimal. |
| 3.2 | `auto_suspend >= 60 AND auto_suspend < 300 AND frequency of resumes is high` | Any | `ALTER WAREHOUSE {wh} SET AUTO_SUSPEND = 300;` | Median provisioning wait is {median_provisioning_ms}ms despite auto_suspend={auto_suspend}s. Workload pattern has frequent gaps > {auto_suspend}s but < 5min. Increasing suspend to 300s will keep the warehouse warm between bursts. |
| 3.3 | `auto_resume = FALSE` | Any | `ALTER WAREHOUSE {wh} SET AUTO_RESUME = TRUE;` | Median provisioning wait is {median_provisioning_ms}ms. AUTO_RESUME is disabled -- queries cannot start until the warehouse is manually resumed. Enable AUTO_RESUME to eliminate manual bottleneck. |
| 3.4 | `auto_suspend >= 300 AND is_gen2 = FALSE` | Any (if Gen2 eligible) | `ALTER WAREHOUSE {wh} SET WAREHOUSE_TYPE = 'STANDARD' RESOURCE_CONSTRAINT = 'STANDARD_GEN_2';` | Median provisioning wait is {median_provisioning_ms}ms despite generous auto_suspend. Gen2 warehouses provision faster due to improved infrastructure. Consider migrating to Gen2. (Note: Gen2 migration may require testing) |
| 3.5 | `is_multicluster = TRUE AND min_cluster_count = 0` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MIN_CLUSTER_COUNT = 1;` | Median provisioning wait is {median_provisioning_ms}ms. MCW is configured with min_cluster_count=0, meaning all clusters can fully suspend. Setting min=1 keeps one cluster warm to serve initial queries instantly. Trade-off: ~{credits_per_hour}/hr idle cost during non-use periods. |

---

## 4. SPILLAGE (Priority: Medium-High -- performance and potential remote spill cost)

**Signal:** `total_gb_spilled_local > 10 OR total_gb_spilled_remote > 0`

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 4.1 | `total_gb_spilled_remote > 0` (any remote spill) | Any | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_up}';` | Remote spillage detected ({total_gb_spilled_remote} GB in 30 days). Remote spill writes to cloud storage, adding significant latency and egress cost. Scaling up from {current_size} provides more local SSD cache before spilling remotely. |
| 4.2 | `total_gb_spilled_local > 50 AND total_gb_spilled_remote = 0 AND size < 'XLARGE'` | Any | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_up}';` | Heavy local spillage ({total_gb_spilled_local} GB in 30 days) on a {current_size} warehouse. Queries are exceeding available RAM and spilling to local SSD. Scaling up doubles available memory and will reduce or eliminate spillage. |
| 4.3 | `total_gb_spilled_local > 50 AND size >= 'XLARGE'` | Any | No DDL -- SQL optimization recommended | Heavy local spillage ({total_gb_spilled_local} GB in 30 days) on a {current_size} warehouse. At this size, further scaling has diminishing returns. Review query SQL for: wide JOINs missing filters, unnecessary columns in SELECT *, exploding CTEs, or missing partition pruning. |
| 4.4 | `total_gb_spilled_local BETWEEN 1 AND 50 AND spill_trend = 'Worsening'` | Any | Monitor -- SQL review first | Moderate local spillage ({total_gb_spilled_local} GB, trending worse). Profile the top spilling queries before scaling -- a SQL fix (adding filters, reducing join width) is cheaper than a permanent size increase. |
| 4.5 | `total_gb_spilled_local BETWEEN 1 AND 50 AND spill_trend != 'Worsening'` | Any | Stable -- no action | Minor local spillage ({total_gb_spilled_local} GB, trend stable). Local SSD spill has minimal performance impact at this volume. Continue monitoring. |
| 4.6 | `is_snowpark_optimized = FALSE AND spillage is from Snowpark/Python UDFs` | Any | `ALTER WAREHOUSE {wh} SET WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED';` | Spillage appears driven by Snowpark/Python workloads. Snowpark-optimized warehouses provide 16x memory per node for in-memory processing. (Note: higher credit rate -- 1.5x) |

---

## 5. OVERSIZED WAREHOUSE (Priority: Medium -- cost savings opportunity)

**Signal:** `avg_query_load_pct < 50 AND median_execution_ms < 500 AND overload_to_elapsed_ratio < 0.01`

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 5.1 | `is_multicluster = TRUE AND max_cluster_count > 1 AND avg_query_load_pct < 30` | Enterprise+ | `ALTER WAREHOUSE {wh} SET MAX_CLUSTER_COUNT = 1;` | Warehouse is oversized -- running at {avg_query_load_pct}% avg load with negligible queuing. Multi-cluster is unnecessary at this concurrency level. Switching to single-cluster saves the MCW overhead and simplifies cost attribution. |
| 5.2 | `is_multicluster = FALSE AND warehouse_size != 'XSMALL' AND median_execution_ms < 200` | Any | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_down}';` | Warehouse is oversized -- median query execution is {median_execution_ms}ms with only {avg_query_load_pct}% load. Scaling down from {current_size} to {next_size_down} halves credit consumption with minimal performance impact. |
| 5.3 | `is_multicluster = FALSE AND warehouse_size = 'XSMALL'` | Any | Stable -- already at minimum | Warehouse is at minimum size (X-Small). Load is low ({avg_query_load_pct}%) -- consider consolidating this warehouse's workloads into another warehouse if feasible. |
| 5.4 | Oversized BUT `dml_pct > 20` AND heavy writes | Any | Monitor -- do not scale down | Warehouse appears oversized by query metrics but has significant DML load ({dml_pct}%). Write-heavy workloads benefit from larger warehouse sizes for COPY/INSERT performance even when concurrency metrics are low. |

---

## 6. EXPENSIVE QUERY COST GROWTH (Priority: Medium -- creeping cost)

**Signal:** `cost_trend = 'Worsening' AND estimated_annual_cost_usd > threshold`

| # | Config Check | Edition Constraint | Recommendation (DDL) | Recommendation Reason |
|---|---|---|---|---|
| 6.1 | Query runs many times/day AND avg_elapsed_sec < 10 | Any | No DDL -- scheduling/consolidation recommended | Query {node_id} runs {total_runs_30d} times at ${estimated_annual_cost_usd}/yr (trending worse). Execution is fast ({avg_elapsed_sec}s) -- cost is driven by frequency. Consider: reducing run frequency, combining with other models in a single dbt run, or implementing incremental materialization. |
| 6.2 | Query runs few times AND avg_elapsed_sec > 60 AND spillage on same WH | Any | `ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{next_size_up}';` | Query {node_id} costs ${estimated_annual_cost_usd}/yr with avg runtime {avg_elapsed_sec}s. This warehouse also shows spillage -- a size increase will both reduce execution time (halving credits per run) and eliminate spill overhead. Net cost may decrease despite higher per-hour rate. |
| 6.3 | Query runs few times AND avg_elapsed_sec > 60 AND no spillage | Any | No DDL -- SQL optimization recommended | Query {node_id} costs ${estimated_annual_cost_usd}/yr with avg runtime {avg_elapsed_sec}s (trending worse). No spillage detected -- long runtime is likely due to full table scans, missing clustering keys, or unoptimized joins. Profile the query plan. |
| 6.4 | `is_gen2 = FALSE` AND query is compute-bound | Any | Consider Gen2 migration | Query {node_id} costs ${estimated_annual_cost_usd}/yr. Warehouse is Gen1 -- Gen2 warehouses offer improved query performance at the same credit rate. Benchmark this workload on Gen2. |

---

## 7. STABLE / NO ACTION

**Signal:** None of the above thresholds are breached

| # | Config Check | Recommendation | Recommendation Reason |
|---|---|---|---|
| 7.1 | All metrics within healthy ranges | Stable -- no sizing change recommended | Metrics within healthy ranges over the last 30 days. idle_credit_pct={x}%, overload_ratio={y}, provisioning_ms={z}, spillage={w}GB. Continue monitoring. |
| 7.2 | Metrics borderline (e.g., idle 15-20%) | Monitor -- approaching threshold | One or more metrics are approaching action thresholds: {list}. No action required yet but schedule a review in 2 weeks. |

---

## Additional Signals NOT Currently Measured (to add)

| Signal | Source | What It Enables |
|--------|--------|-----------------|
| `queued_overload_time` (per query, aggregated) | SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY | More granular overload detection -- can identify specific time-of-day or user patterns |
| `queued_provisioning_time` (per query, aggregated) | SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY | Distinguish between cold-start types (full suspend vs cluster add) |
| `avg_running` / `avg_queued_load` | SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY | Time-series view of warehouse saturation -- enables "peak hour" recommendations |
| `snowflake_edition` | SHOW ORGANIZATION ACCOUNTS or param check | Gate MCW recommendations (critical for Standard edition accounts) |
| `resume_count` / `suspend_count` (per day) | Derivable from WAREHOUSE_EVENTS_HISTORY (RESUME_CLUSTER / SPINUP_CLUSTER events) | Quantify cold-start frequency for provisioning recommendations |
| `cluster_utilization` (per cluster in MCW) | WAREHOUSE_LOAD_HISTORY broken by interval | Detect if MCW is spinning clusters that sit idle (scaling_policy tuning) |
| `query_acceleration_eligible` | QUERY_HISTORY.QUERY_ACCELERATION_MAX_SCALE_FACTOR | Recommend Query Acceleration Service instead of size-up for long-tail queries |

---

## Size Ladder Reference (for DDL generation)

```
XSMALL -> SMALL -> MEDIUM -> LARGE -> XLARGE -> 2XLARGE -> 3XLARGE -> 4XLARGE -> 5XLARGE -> 6XLARGE
Credits/hr:  1       2        4       8        16        32         64        128       256       512
```

Next-size-up/down logic should clamp at boundaries and account for credit doubling when calculating cost impact in the recommendation reason.
