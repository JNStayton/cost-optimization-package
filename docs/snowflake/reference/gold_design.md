# Gold Layer Design — Snowflake Cost Optimization

This document specifies the design, methodology, and rules for the gold-layer dashboard views that sit on top of the domain-specific fact models.

---

## 1. Purpose

The gold layer answers: **"What should I do next?"**

It transforms raw domain-specific recommendations (warehouse sizing, spillage, materialization, clustering, AI spend) into:
- **Prioritized action items** ranked by estimated dollar impact
- **Cross-domain correlations** that identify root causes spanning multiple domains
- **Effort-classified backlog items** ready for human sprint planning or agent automation
- **KPI summaries** for dashboard consumption

### Two audiences

| Audience | What they need | How they consume |
|----------|---------------|-----------------|
| **Humans** (platform engineers, analytics engineers) | Ranked action items, dollar estimates, quick wins highlighted | Dashboard tiles, filtered views, drill-downs |
| **Agents** (Cortex Agents, CI automation, ticket bots) | Structured fields, self-contained rows, node_id for code navigation | SQL queries against gold views, snowflake_ddl/dbt_model_config fields for auto-apply |

---

## 2. Views

### `vw_snowflake__top_recommendations`

**The flagship view.** P1+P2 recommendations across all domains, deduplicated per entity (highest savings as representative). Includes related signal count for context.

| Column | Type | Description |
|--------|------|-------------|
| `priority_rank` | int | Dense rank by priority_tier ASC, savings DESC |
| `domain` | string | warehouse / materialization / clustering / ai |
| `signal_id` | string | Machine-readable signal identifier |
| `priority_tier` | int | Per-entity relative priority (1 = do first) |
| `node_id` | string | Logical dbt model identifier (nullable) |
| `model_name` | string | Human-readable model name |
| `warehouse_name` | string | Relevant warehouse |
| `recommendation` | string | Short action text |
| `recommendation_reason` | string | Detailed evidence with metrics |
| `estimated_annual_cost_usd` | float | "If unchanged, expect this yearly cost" |
| `estimated_annual_savings_usd` | float | "If fixed, save this amount yearly" |
| `related_signals_count` | int | Other signals for this entity (shows compound opportunity) |
| `snapshot_date` | date | When this analysis was produced |

Filters to P1+P2 per entity. Ordered by priority_tier, then savings DESC. One row per entity (deduplicated to highest-savings representative).

---

### `vw_snowflake__cost_savings_summary`

**KPI card view.** One row per domain showing total opportunity.

| Column | Type | Description |
|--------|------|-------------|
| `domain` | string | warehouse / materialization / clustering / ai |
| `total_recommendations` | int | Count of actionable items in this domain |
| `quick_win_count` | int | Count where effort_category = 'config_change' |
| `estimated_annual_cost_usd` | float | Total current projected cost for this domain |
| `estimated_annual_savings_usd` | float | Total savings if all recommendations applied |
| `savings_pct` | float | savings / cost as a percentage |
| `top_recommendation` | string | Preview text of the #1 savings item |
| `top_recommendation_savings_usd` | float | Dollar amount of the single biggest opportunity |

Expected output: 4-5 rows (one per domain with data).

---

### `vw_snowflake__optimization_backlog`

**Sprint planning / agent intake view.** Full optimization inventory across ALL priority tiers.

Includes all detected signals (actionable + monitor). Users filter by `priority_tier` to focus on specific tiers. Each row is self-contained — an agent can create a ticket from any single row.

Key columns:
- `priority_tier`: per-entity relative priority (1 = do first for this entity)
- `hierarchy_rank`: fixed position in the optimization decision hierarchy
- `signal_id`: machine-readable signal identifier
- `effort_category`: config_change / actionable_review / sql_refactor / investigation
- `action_type`: 'snowflake_ddl' / 'dbt_config' / 'investigation' — makes clear which column to use
- `dbt_model_config`: copy-pasteable dbt config block
- `snowflake_ddl`: SQL to execute directly in Snowflake
- `blocking_signals` / `next_evidence_needed`: for investigate items

Ordered by: `priority_tier ASC`, then `estimated_annual_savings_usd DESC`.

---

### `vw_snowflake__dbt_model_optimizations`

**dbt engineer view.** Only ACTIONABLE items — things with templates ready to copy-paste and test. Excludes `investigate` and `do_not_recommend` statuses (those are in the backlog).

Covers: materialization changes, clustering keys, and incremental configs with `actionable_review` status.
Ordered by `model_name, priority_tier` so all optimizations for a model are grouped together.

Key columns:
- `priority_tier`: per-entity relative priority (1 = do first for this model based on hierarchy)
- `suggested_clustering_key`: ready-to-use clustering key columns
- `dbt_model_config`: copy-paste config block (materialization, incremental, cluster_by)
- `identified_unique_key`: confirmed unique key name (NULL for append strategies)
- `incremental_confidence_score`: 0-100 confidence in incremental template (NULL if no strategy proposed)
- `incremental_blocking_signals`: factors reducing confidence
- `environment_ids`: array of dbt_cloud_environment_ids where this model exists

---

### `vw_snowflake__warehouse_optimizations`

**Snowflake admin view.** One row per (warehouse, signal category). Config recs are standalone. Model-level signals (spillage, expensive queries) are aggregated with linked model context.

Key columns:
- `warehouse_current_size`: authoritative size from WAREHOUSE_EVENTS_HISTORY
- `warehouse_category`: standard / gen2 / multi_cluster / adaptive
- `signal_id`: signal type (idle_reduce_auto_suspend, spillage_moderate_worsening, etc.)
- `priority_tier`: per-warehouse relative priority
- `snowflake_ddl`: concrete ALTER WAREHOUSE DDL (for config recs)
- `affected_model_count`: count of models contributing to this signal
- `affected_models`: comma-separated list of affected model names
- `recommendation_reason`: for aggregated signals, includes "resolve model-level optimizations first" guidance

Expensive queries are surfaced separately in `vw_snowflake__top_expensive_queries`.

---

### `vw_snowflake__top_expensive_queries`

**Cost accountability view.** Top 10 most expensive recurring queries within the project scope, enriched with co-occurring optimization signals.

Key columns:
- `query_hash`: unique identifier for this recurring query pattern
- `estimated_annual_cost_usd`: projected annual cost at current run rate
- `recommendation`: actionable recommendation for this query
- `model_name` / `node_id`: the dbt model this query builds (when attributable)
- `co_occurring_fixes`: when co-signals exist, shows "Actionable — clustering + incremental optimization(s) available"
- `co_signal_count`: number of co-occurring optimization signals for this model

---

### `vw_snowflake__top_spillage_models`

**Performance engineering view.** Models causing the most memory spillage, with dbt platform run traceability.

Key columns:
- `model_name` / `node_id`: the dbt model
- `model_source`: 'project' or 'installed_package'
- `total_gb_spilled`: aggregate spillage across runs
- `spill_trend`: Worsening / Improving / Stable
- `priority_tier`: per-entity relative priority from int_all_recommendations
- `signal_id`: primary (lowest-tier) spillage signal for this model
- `spillage_signal_count`: number of distinct spillage signals (e.g., scale_up + moderate)
- `all_spillage_signals`: comma-separated list of all spillage signal_ids
- `last_spilling_run_id` / `last_spilling_job_id`: dbt platform run and job IDs for the most recent spilling execution
- `warehouse_name`: warehouse where spillage occurred

Filtered to models only (dbt_model IS NOT NULL). Includes installed packages since they run on your warehouse.

---

### `vw_snowflake__top_queried_models`

**Platform engineering view.** Top 25 most-queried models by SELECT consumption pressure.

Key columns:
- `model_name` / `node_id`: the dbt model
- `total_select_count`: SELECT queries hitting this model in the analysis window
- `total_select_credits`: estimated credits consumed by downstream reads
- `avg_select_duration_sec`: average query duration

Joins `int_snowflake__table_query_stats_daily` to `int_dbt__relations` to identify which models receive the most downstream consumption.

---

### `vw_snowflake__ai_optimizations`

**AI/ML team view.** Model cost (downgrade, prompt bloat, batching), token efficiency (failure rates, I/O ratios, caching), agent cost trends, and spend overview.

Key columns: `service_or_model`, `estimated_annual_savings_usd`

---

### `vw_snowflake__cross_domain_insights`

**Multi-signal correlation view.** Reads from `int_snowflake__all_recommendations` (single source of truth). For each model with 2+ optimization signals from different categories, surfaces the signal array and picks the primary recommendation based on priority_tier.

| Column | Type | Description |
|--------|------|-------------|
| `model_name` | string | The dbt model with multi-signal correlation |
| `signals` | string | Comma-separated signal categories (e.g., 'spillage,clustering,incremental') |
| `signal_count` | int | Number of distinct signal categories (2+ for this view) |
| `primary_recommendation` | string | Recommendation text from lowest priority_tier signal |
| `primary_domain` | string | Domain of the primary recommendation |
| `root_cause` | string | Why these signals co-occur |
| `recommended_action` | string | The model's own top action, plus the view chain addendum (below) |
| `upstream_view_chain` | string | Views and ephemerals the model's builds recompute inline, nearest first, e.g. `int_order_item_summary (ephemeral), int_customer_order_items_geo (view)` |
| `upstream_view_count` | int | How many |
| `chain_recommended_view` | string | The chain's recommended view to materialize (from `fct_snowflake__table_materialization_candidates`) |

Filters to `backlog_status IN ('actionable', 'monitor')` to include spillage and expensive query signals alongside actionable recommendations. Primary recommendation is derived from `min_by(recommendation, priority_tier)` — the signal with the lowest (best) priority_tier for that entity.

Signal categories: spillage, clustering, incremental, materialization, expensive_query, and view_chain. A row needs 2+ distinct categories.

**`view_chain`** marks a table whose builds recompute upstream views and ephemerals inline (`int_snowflake__table_upstream_views`). It counts toward the two-signal minimum: it's effectively a materialization recommendation surfacing on the chain's end table, and it links spillage or cost to the chain.

**Recommended action with a view chain.** The headline stays the model's own top action (materialization → incremental → clustering). When the model also has spillage or an expensive query, the chain is part of the cause, so the action adds the chain's recommended view:

- *Own action + chain + spillage:* "Convert to incremental — …; additionally, materialize `int_order_items_vw` as a table: this model's builds recompute 3 upstream view(s), which adds to its spill." With an expensive query instead: "which adds to its cost."
- *Chain + spillage (or expensive query), no other action:* the chain is the headline: "Materialize `int_order_items_vw` as a table: …"
- *No view to name* (only ephemerals upstream, or no candidate view): "Its builds recompute N upstream view(s), …. See `vw_snowflake__dbt_model_optimizations`."
- *Chain + another action, no spillage or expensive query:* the model's own action, unchanged.

The root cause appends the chain's part the same way ("Builds recompute N upstream view(s) inline, enlarging the working set that spills").

---

### `vw_snowflake__user_level_cost_attribution`

**Manager/chargeback view.** User-level cost attribution across three categories: dbt build users, consumption users (SELECT queries against project models), and AI users.

| Column | Type | Description |
|--------|------|-------------|
| `user_name` | string | Snowflake user |
| `role_name` | string | Primary role |
| `build_credits_30d` | float | Credits from dbt model builds (INSERT/MERGE/CTAS in dbt sessions) |
| `consumption_credits_30d` | float | Credits from SELECT queries against project models |
| `ai_credits_30d` | float | Credits from AI/Cortex usage |
| `combined_credits_30d` | float | Total credits across all categories |
| `estimated_annual_cost_usd` | float | Projected annual cost |
| `primary_warehouse` | string | Warehouse used for builds (builders only) |
| `user_category` | string | builder / consumer / ai_user / mixed |
| `build_query_count` | int | Number of build queries (INSERT/MERGE/CTAS) in last 30 days |
| `consumption_query_count` | int | Number of SELECT queries against project models in last 30 days |
| `ai_query_count` | int | Number of AI/Cortex queries in last 30 days |
| `recommendation` | string | Action/awareness text |
| `credits_from_attribution` | boolean | True when any credits came from `QUERY_ATTRIBUTION_HISTORY` (precise). False = all estimated from elapsed x list rate |

---

## 3. Cost Estimation Methodology

### Credit rate approach: list rate vs. amortized

The package uses two different credit rates depending on the purpose:

**Forward-looking savings/cost estimates** (in `int_snowflake__all_recommendations` and all gold views) use Snowflake's **published list rate** — the fixed credits-per-hour for a warehouse's size, looked up from the warehouse that actually builds each model (via the `model_warehouses` CTE). This is the marginal cost of compute time and correctly estimates what users would save by optimizing. The `warehouse_credits_per_hour` macro maps a size column to the list rate:

| Size | Credits/Hour |
|------|-------------|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |
| 3X-Large | 64 |
| 4X-Large | 128 |
| 5X-Large | 256 |
| 6X-Large | 512 |

The rate is sourced from `int_snowflake__warehouse_config.current_size` (populated by the `refresh_warehouse_config` post-hook from SHOW WAREHOUSES). For warehouses without a known size, the fallback is 1 credit/hour (X-Small) — conservative, underestimates rather than overestimates. The fallback applies when:
- **A model has no build warehouse:** no query in the last 30 days carries its `node_id` in a dbt query comment (for example, a model that hasn't run recently, or one whose queries were issued without dbt's query comment).
- **The warehouse has no known size:** it's missing from `int_snowflake__warehouse_config`, because it has no `WAREHOUSE_CONSISTENT` event in `WAREHOUSE_EVENTS_HISTORY` and isn't visible to `SHOW WAREHOUSES` for the package's role (for example, a warehouse that has since been dropped).
- **The size value isn't recognized:** it isn't one of the sizes in the table above.

**Backwards-looking cost attribution** (in `vw_snowflake__user_level_cost_attribution`) uses **QUERY_ATTRIBUTION_HISTORY** as the primary source, with elapsed x list rate as the fallback:

- **Primary:** `credits_attributed_compute` from `QUERY_ATTRIBUTION_HISTORY` — exact per-query credits that correctly handle multi-cluster, Snowpark-optimized, Gen2, and concurrency splitting. Available on all editions (data from Aug 2024 onward).
- **Fallback:** When QAH has no row (short queries <= ~100ms, or queries before Aug 2024), credits are estimated as `elapsed_time × credits_per_hour / 3600` using the warehouse's list rate. This matches the savings rate exactly.
- The `credits_from_attribution` column flags whether any of a user's credits came from QAH data.

Note: For view materialization recommendations, the build warehouse is used as a proxy for the readers' warehouse. This is accurate for dbt-to-dbt lineage but may understate costs when BI tools read the view from a different warehouse.

**Why the split?** Savings estimates need the list rate because it reflects the actual cost of compute time — a query running for 30 seconds on a Medium warehouse genuinely costs 4/3600 x 30 credits. Using an amortized rate (daily credits / 86,400) would understate savings for any warehouse that doesn't run 24/7. Attribution uses QAH because it correctly handles concurrency (splitting credits across concurrent queries) and warehouse-type pricing (Gen2, Snowpark-optimized, MCW). Where QAH isn't available, the list rate fallback prices each query as though it had the warehouse to itself — an upper bound that ensures completeness.

**Known limitations of the list rate:**
- **Gen2 and Snowpark-optimized warehouses** cost more per hour than the standard list rate. QAH handles these correctly; the list rate fallback does not.
- **Multi-cluster warehouses** cost a multiple of the list rate per cluster. QAH splits this correctly; the list rate prices at one cluster.
- **Concurrency:** Pricing a query's elapsed time at the full list rate assumes it had the warehouse to itself, so it's an upper bound when queries run concurrently. QAH splits correctly.

### Dollar conversion

All dollar estimates use the configurable `credit_rate_usd` variable (default: $2/credit). Users should set this to their contract rate.

```
estimated_annual_cost_usd = annual_credits × credit_rate_usd
```

### Per-domain cost estimation

#### Warehouse idle credit savings

Idle credit savings are computed per-recommendation-key using actual event data from `WAREHOUSE_EVENTS_HISTORY`, not hardcoded assumptions:

| Recommendation Key | Savings Formula |
|---|---|
| `idle_reduce_auto_suspend` | `autosuspend_cycles_30d × (current_auto_suspend - 60) / 3600 × credits_per_hour × 12 × credit_rate_usd` |
| `idle_switch_scaling_policy` | `mcw_spindown_cycles_30d × 150 / 3600 × credits_per_hour × 12 × credit_rate_usd` |
| `idle_reduce_max_clusters` | Same as scaling policy (fewer clusters = fewer spindown idle periods) |
| `idle_reduce_min_clusters` | `(min_cluster_count - 1) × credits_per_hour × idle_pct × 720 × 12 × credit_rate_usd` |
| `idle_consolidate_underloaded` | `total_idle_credits_30d × 12 × credit_rate_usd` (full elimination) |
| `idle_consolidate_standard` | `total_idle_credits_30d × 0.5 × 12 × credit_rate_usd` (conservative 50%) |
| `idle_enable_mcw_bursty` | null (adds cost; benefit is reduced queuing, not idle savings) |

The `autosuspend_cycles_30d` and `mcw_spindown_cycles_30d` come from `int_snowflake__warehouse_suspend_cycles`, derived from SUSPEND_WAREHOUSE and SUSPEND_CLUSTER events in `WAREHOUSE_EVENTS_HISTORY`.

**Why not use total_idle_credits?** On high-throughput warehouses, most idle credits are normal inter-query overhead (the warehouse is running but between query executions). Auto-suspend changes only affect the idle time during actual suspend cycles, not the inter-query overhead.

#### Warehouse sizing (scale down / oversized)

| Metric | Formula |
|---|---|
| Current annual cost | `total_credits_30d × 12 × credit_rate_usd` |
| Savings (oversized / scale down) | `total_credits_30d × 0.50 × 12 × credit_rate_usd` |

#### Expensive queries

| Metric | Formula |
|--------|---------|
| Current annual cost | `estimated_annual_cost_usd` (already computed in fact model) |
| Savings | Conservative 20% reduction estimate (refactoring typically achieves 20-80%) |

#### Materialization (view → table)

| Metric | Formula |
|--------|---------|
| Current annual cost | `(select_count + downstream_build_count) × recompute_cost_s × credits_per_hour / 3600 × (365 / lookback_days) × credit_rate_usd` |
| Savings | `max(select_count + downstream_build_count - view_build_runs, 0) × recompute_cost_s × credits_per_hour / 3600 × (365 / lookback_days) × credit_rate_usd` |

`lookback_days` is `table_materialization_lookback_days` (default 14), the window all three counts cover, so `365 / lookback_days` annualizes them.

Logic: a view's query runs on every read (`select_count`) and on every build of a table it feeds (`downstream_build_count`). Materialized, it runs once per dbt run instead (`view_build_runs`, the number of times dbt created the view in the window). Savings are the runs that go away, so they can't exceed the cost.

- **`downstream_build_count`** counts builds of every table downstream of the view through views and ephemerals only: CTAS and MERGE statements tagged with the downstream model's `node_id` in dbt's query comment. Known edges: an incremental model merged through a temporary table counts twice per run; one appended through a temporary view counts zero.
- **`view_build_runs`** counts CREATE_VIEW statements tagged with the view's `node_id`, minimum 1.
- **`recompute_cost_s`** is the seconds one run of the view's query takes: measured by the view probe (`select hash_agg(*) from <view>`, result cache off, in `int_snowflake__view_probe`) when there is one, else the view's average read duration (`recompute_cost_source` says which). Read durations understate a view deep in a chain: a filtered read of it is cheap, but a downstream build recomputes all of it, plus every view above it. Each downstream build is charged this time, not the downstream model's whole build time: materializing removes the view's share of the build, not the build.

**One view per chain.** Every view in a chain gets the same downstream builds, so materializing any of them removes that recompute: they're alternatives, not additions. For each table at the end of a chain, the candidate view with the highest net savings (`net_recompute_s_saved`; ties go to the view nearest the table) is `chain_role = 'recommended'` and actionable. The others are `'alternative'`, with status `monitor` and a reason naming the recommended view, so the cost-savings summary counts the chain once. A view feeding several tables is recommended if it wins for any of them. Ephemerals are never candidates (no relation to read or probe); a view below an ephemeral is probed with the ephemeral inlined, so it's credited with the ephemeral's work.

#### Spillage

| Metric | Formula |
|--------|---------|
| Current annual cost | `spilling_execution_s × credits_per_hour / 3600 × (365 / spillage_lookback_days) × credit_rate_usd` |
| Savings, SQL refactor | `spill_blocked_s_total × credits_per_hour / 3600 × (365 / spillage_lookback_days) × credit_rate_usd`; null without operator stats |
| Savings, scale-up | null: see hours saved and cost change |
| Hours saved, scale-up | `spilling_execution_s × (1 − 1 / (2 × eff)) / 3600 × (365 / spillage_lookback_days)` |
| Cost change, scale-up | `spilling_execution_s × credits_per_hour / 3600 × (1 / eff − 1) × (365 / spillage_lookback_days) × credit_rate_usd` (positive = costs more) |
| Hours saved, SQL refactor | `spill_blocked_s_total / 3600 × (365 / spillage_lookback_days)` |

`spilling_execution_s` (on `fct_snowflake__warehouse_performance_recommendations`) is the measured runtime of the table's spilling queries in the `spillage_lookback_days` window (default 30), attributed to the table through `ACCESS_HISTORY`, the same attribution as the spilled GB. The table's warehouse is the one its spilling queries spilled most on, and `credits_per_hour` is that warehouse's list rate. The aggregate (per-warehouse) spillage recommendation uses the same calculation over all of the warehouse's spilling queries.

**Scale-ups trade credits for time.** A calibration on spilling dbt builds halved their runtime at roughly the same credits, so a scale-up's dollar savings can't be told apart from zero, and its savings are null. Instead, `estimated_annual_hours_saved` and `estimated_annual_cost_change_usd` state the trade, and the reason text says it ("about 1.7x faster (… hours a year), about +16% credits"). `eff` is the efficiency of the step up from the warehouse's current size (`warehouse_scale_up_efficiency`), from the Snowflake Summit session "Beyond Code: Right-Sizing Your Warehouse" (one complex query on every size):

| Step up from | X-Small, Small | Medium | Large | X-Large | 2X-Large | 3X–5X-Large | 6X-Large |
|---|---|---|---|---|---|---|---|
| eff | 1.00 | 0.86 | 0.92 | 0.86 | 0.85 | 0.81 | null |

At `eff`, the runtime falls to `T / (2 × eff)` at twice the rate. Our own calibration measured 0.91–0.97 for X-Small → Small on spilling queries. Scaling up reduces spill; eliminating it may also need the model's SQL or materialization changed.

**SQL refactors are priced from measured spill.** The `extract_spill_evidence` post-hook reads `GET_QUERY_OPERATOR_STATS` for a sample of each spilling table's queries (the most recent per query shape, last 14 days, into `int_snowflake__query_spill_evidence`). For each, `spill_blocked_s` is the execution time × the disk I/O share of the operators with spilling statistics; other disk I/O (scans, cache reads) isn't spill. The sample is scaled to all the table's spilling queries: `spill_blocked_s_total = sampled blocked_s × spilling_execution_s / sampled execution_s`. Blocked time is a lower bound on what removing the spill saves. Without operator stats (no MONITOR on the warehouse, or queries older than 14 days) the savings and hours are null.

**Job-level spillage.** Per-table and per-warehouse spillage miss a common pattern: a few models dominate a dbt job's build time by spilling. `int_snowflake__dbt_job_spillage` rolls each job's builds up over the window (jobs from `dbt_cloud_job_id` in dbt's query comment; for runs outside the dbt platform, add `invocation_id` to your `query-comment` and each invocation is treated as a one-run job):

| Model share (spilling / built) | Time share (spilling models' build time / all) | Signal | Status | Rank | Surfaces in |
|---|---|---|---|---|---|
| ≤ 25% | ≥ 75% | `spillage_route_models` | actionable | 4 | `vw_snowflake__dbt_model_optimizations` (one row per routed model, with a `snowflake_warehouse` config) |
| > 25% | ≥ 75% | `spillage_job_scale_up` | actionable | 4 | `vw_snowflake__warehouse_optimizations` (entity `dbt job <id>`) |
| either | 25–75% | the same signal, by model share | monitor | 5 | same |
| any | < 25% | none | | | |

The thresholds are the `spillage_job_*` vars. Routing and sizing up are alternatives for the same job, so a job gets one or the other. Routing names the size one up, not a warehouse: the package can't tell who owns a warehouse or whether your role can use it, so the config is `snowflake_warehouse='<larger warehouse>'` for you to fill in. A job size-up includes DDL unless other jobs share the warehouse; then it recommends a dedicated warehouse instead of resizing the shared one. Both are priced like scale-ups: cost from build time (the routed model's, or the whole job's), savings null, and hours saved and cost change from the efficiency curve.

Null savings aren't demoted by `min_annual_savings_usd`, so scale-ups stay actionable. Earlier versions estimated cost from spilled GB (0.5 s per local GB, 5 s per remote GB, 70% savings); those constants were invented and are gone.

#### Clustering

| Metric | Formula |
|--------|---------|
| Current annual cost | `select_count × avg_query_duration_s × credits_per_hour / 3600 × (365 / lookback_days) × credit_rate_usd` |
| Savings | `current_cost × (filter_query_count / total_queries_analyzed) × (scan_ratio - 1/distinct_values)` |

`lookback_days` is `clustering_candidates_lookback_days` (default 7), the window `select_count` covers.

The savings are weighted by two data-driven factors:
1. **Filter proportion**: `filter_query_count / total_queries_analyzed` — only queries that filter on the recommended clustering key benefit from partition pruning. Both counts come from the queries the `extract_operator_evidence` hook analyzed (a sample of up to `clustering_key_operator_queries_per_table` per run), so the share is measured within that sample, not against all reads
2. **Cardinality-based scan reduction**: `scan_ratio - 1/K` where K = distinct values for the clustering key — theoretical post-clustering scan ratio based on actual key cardinality

This replaces the prior fixed-floor assumption (0.2) with values derived from actual query operator evidence and column cardinality profiling.

Note: Clustering itself has a maintenance cost (auto-reclustering credits). We do NOT subtract this from savings because it's highly variable and depends on DML patterns. The savings estimate is gross, not net.

#### AI spend

| Metric | Formula |
|--------|---------|
| Current annual cost | `projected_annual_cost_usd` (already computed in fact model) |
| Savings (model downgrade) | Difference in per-token cost between current model and recommended cheaper model |
| Savings (batch processing) | Estimated 30% reduction from batch pricing (Snowflake offers batch discounts) |

---

## 4. Effort Classification Rules

Each recommendation is classified into one of three effort categories:

### `config_change` — Quick wins (minutes to apply)

| Source Model | Recommendation Pattern | Actionable SQL |
|---|---|---|
| Warehouse sizing | Scale down | `ALTER WAREHOUSE <name> SET WAREHOUSE_SIZE = '<one_size_down>'` |
| Warehouse sizing | Enable Gen2 | `ALTER WAREHOUSE <name> SET RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'` |
| Warehouse sizing | Enable MCW | `ALTER WAREHOUSE <name> SET MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = <n>` |
| Materialization v2 | Materialize as TABLE | `{{ config(materialized='table') }}` |
| Incremental config | Strategy recommended (with template) | The `dbt_model_config` column has the ready-to-paste block |
| Clustering candidates | Add clustering key | `ALTER TABLE <fqn> CLUSTER BY (<recommended_keys>)` |
| AI spend | Model downgrade | Change model name in application code |
| Warehouse sizing | Reduce auto-suspend | `ALTER WAREHOUSE <name> SET AUTO_SUSPEND = 60` |

### `sql_refactor` — Needs investigation (hours to days)

| Source Model | Recommendation Pattern | Investigation Path |
|---|---|---|
| Warehouse spillage | Heavy spillage from query patterns | Review model SQL for wide joins, missing filters, cross joins |
| Expensive queries | High credit consumption | Profile the query, identify optimization opportunities |
| AI spend | Prompt bloat detected | Trim prompt engineering, reduce input token count |

### `architecture` — Design work required (days to weeks)

| Source Model | Recommendation Pattern | Action |
|---|---|---|
| Incremental candidates | Convert to incremental (complex) | Model restructure: choose strategy, find unique keys, add filter column |
| Materialization v2 | Deep view chain (4+ hops) | May require DAG restructure to reduce chain depth |
| Multiple overlapping issues | Same model has 3+ recommendations | Holistic redesign needed |

---

## 5. Cross-Domain Correlations

These are detected by joining fact models on `table_fqn` or `node_id` and looking for overlapping recommendations.

### Correlation 1: Spillage + Clustering Candidate

**Detection:** Same `table_fqn` appears in both `fct_warehouse_spillage_recommendations` AND `fct_table_clustering_candidates` (where `is_candidate = true`).

**Root cause:** Full table scans (high scan_ratio) force the query to process all micropartitions, overflowing memory and spilling to disk.

**Recommended fix order:** Cluster first. Clustering reduces scan volume, which likely eliminates the spillage without needing a warehouse scale-up.

**Evidence:** "Table has {scan_ratio}% scan ratio AND {gb_spilled} GB spillage. Clustering on {recommended_keys} would reduce both."

---

### Correlation 2: View in Chain + Downstream Spillage

**Detection:** Same `table_fqn` appears in `fct_table_materialization_candidates` (recommendation = 'Materialize as TABLE') AND a downstream table in `fct_warehouse_spillage_recommendations`.

**Root cause:** The view recomputes on every downstream build, creating large intermediate result sets that spill.

**Recommended fix order:** Materialize the view. Eliminates recomputation, which eliminates the intermediate result set that causes spillage.

**Evidence:** "View {model_name} is {hops} hops from downstream table that spills {gb_spilled} GB. Materializing eliminates cascading recomputation."

---

### Correlation 3: Expensive Query + Oversized Warehouse

**Detection:** A `query_hash` from `fct_expensive_query_recommendations` runs on a warehouse from `fct_warehouse_sizing_recommendations` where recommendation = 'Scale down'.

**Root cause:** The warehouse is oversized (queries don't need that much compute), but individual queries are still expensive (they're inefficient SQL, not undersized-warehouse problems).

**Recommended fix order:** Fix the query first. Scaling down the warehouse would make the expensive query even slower. The warehouse appears oversized because most queries are tiny, but the expensive one is masking the issue.

**Evidence:** "Warehouse {name} is oversized (median exec {sec}s) but query {hash} costs {credits} credits/month. The query needs refactoring, not the warehouse."

---

### Correlation 4: Same Model in Multiple Environments

**Detection:** Same `node_id` has recommendations in 2+ environments (via `int_snowflake__dbt_relation_history`).

**Root cause:** The model is inefficient regardless of environment — the problem is in the code, not the infrastructure.

**Recommended fix order:** Fix at the source (model SQL), which propagates to all environments automatically.

**Evidence:** "Model {model_name} has {recommendation} in {n} environments ({targets}). Fix the model code — all environments benefit."

---

### Correlation 5: Expensive Query + Incremental Candidate

**Detection:** A `dbt_node_id` from `fct_expensive_query_recommendations` matches a `dbt_model` in `fct_incremental_materialization_candidates`.

**Root cause:** The query is expensive because it fully rebuilds a large table every run. Converting to incremental would process only new/changed data.

**Recommended fix order:** Convert to incremental. This addresses both the expensive query cost (smaller working set per run) and the materialization inefficiency.

**Evidence:** "Query for model {model_name} costs {credits}/month and rebuilds {table_size_gb} GB with {rebuild_redundancy_rate}% redundancy. Incremental would process only delta."

---

### Correlation 6: Spillage + Incremental Candidate

**Detection:** Same `table_fqn` appears in both `fct_warehouse_spillage_recommendations` AND `fct_incremental_materialization_candidates`.

**Root cause:** Full table rebuilds process the entire dataset, overflowing memory. Incremental would reduce the working set size.

**Recommended fix order:** Convert to incremental. Smaller working set per run means less memory pressure, likely eliminating spillage.

**Evidence:** "Table {table_fqn} spills {gb_spilled} GB during builds AND has {rebuild_redundancy_rate}% rebuild redundancy. Incremental would reduce working set and eliminate spillage."

---

### Correlation 7: High AI Spend + User Concentration

**Detection:** `fct_ai_spend_overview` shows high total_credits AND `fct_ai_user_spend_recommendations` identifies a single user with >50% of spend.

**Root cause:** AI costs aren't systemic — one user/workflow is driving the majority of consumption.

**Recommended fix order:** Address the concentrated user's patterns (rate limit, model downgrade for their use case, or optimize their prompts).

**Evidence:** "AI spend is {credits}/month. User {user_name} accounts for {pct}% of total. Optimizing their usage alone would save {savings}/year."

---

### Correlation 8: Materialization Candidate + Expensive Downstream Query

**Detection:** A `table_fqn` in `fct_table_materialization_candidates` has a downstream table whose build queries appear in `fct_expensive_query_recommendations`.

**Root cause:** An expensive downstream query is expensive partly because it recomputes an upstream view every time it runs.

**Recommended fix order:** Materialize the upstream view. The downstream query's cost drops because it reads a pre-computed table instead of triggering cascading view expansion.

**Evidence:** "View {model_name} is referenced by expensive query {hash} (${cost}/year). Materializing eliminates {select_count} recomputations/period."

---

### Correlation 9: Clustering Candidate + Expensive Query

**Detection:** A `table_fqn` in `fct_table_clustering_candidates` (with high scan_ratio) also has queries in `fct_expensive_query_recommendations`.

**Root cause:** Queries are expensive because they scan the full table (no pruning). Clustering would allow partition pruning.

**Recommended fix order:** Add clustering key. Reduces scan cost for all queries against this table, including the expensive ones.

**Evidence:** "Table {table_fqn} has {scan_ratio}% scan ratio and expensive queries costing {credits}/month. Clustering on {keys} would reduce scan volume and query cost."

---

### Correlation 10: Oversized Warehouse + Low Query Volume

**Detection:** `fct_warehouse_sizing_recommendations` shows 'Scale down' AND `total_queries_30d` is below a threshold (e.g., < 1000) AND `avg_idle_credit_pct > 30%`.

**Root cause:** The warehouse isn't just oversized — it's barely used but stays running (high idle credits). Auto-suspend tuning is needed alongside sizing.

**Recommended fix order:** Reduce auto-suspend timeout first (immediate credit reduction), then evaluate sizing.

**Evidence:** "Warehouse {name} has {queries} queries/month with {idle_pct}% idle credits ({idle_credits} wasted). Reduce auto-suspend from current to 60s, then evaluate sizing."

---

### Correlation 11: Incremental + Materialization in Same Lineage

**Detection:** A `table_fqn` in `fct_table_materialization_candidates` is upstream (in `int_dbt__relations.parent_models`) of a `table_fqn` in `fct_incremental_materialization_candidates`.

**Root cause:** A view feeds into a table that should be incremental. Both changes together compound: materializing the view reduces the incremental model's scan cost, and making it incremental reduces redundant rebuilds.

**Recommended fix order:** Materialize the view first (quick config change), then convert the downstream to incremental (architecture work).

**Evidence:** "View {view_model} feeds table {table_model}. Materializing the view AND converting to incremental would compound savings: eliminated recomputation + eliminated redundant rebuilds."

---

### Correlation 12: Package Self-Referential Spillage

**Detection:** `fct_warehouse_spillage_recommendations` contains rows where `package_name = 'dbt_cost_optimization'`.

**Root cause:** The optimization package's own models are spilling because they query large ACCOUNT_USAGE views. This is a "heal thyself" signal.

**Recommended fix order:** Scale up the build warehouse for package model runs, or increase the warehouse's auto-suspend timeout to avoid cold starts.

**Evidence:** "Package model {model_name} spills {gb_spilled} GB on warehouse {warehouse_name}. Consider using a larger warehouse for the cost optimization job, or materializing upstream intermediates."

---

## 6. Environment Handling

### How deduplication works

When multiple environments have the same recommendation for the same logical model, the gold layer picks the **highest-impact** environment (most savings/score) regardless of environment label. This avoids guessing which environment is "production" — the most expensive instance is always the most actionable.

### Environment identifiers

| Field | Source | Description |
|-------|--------|-------------|
| `dbt_cloud_environment_id` | Query comment JSON | Unique per dbt platform environment. Not present on dbt platform Studio (development) builds or outside dbt platform. |
| `target_name` | Query comment JSON | Human-readable but unreliable: often "default" in dbt platform, and one Studio session can record both "default" and "dev". |
| `deployed_relation_count` | Derived | Number of deployments of the model: distinct physical tables it has been built into (e.g. a dev schema, a prod schema, and dbt Cloud CI schemas), excluding `dbt_excluded_schemas` / `dbt_excluded_targets`. Set `dbt_excluded_schemas: ['DBT_CLOUD_PR_%']` to leave CI schemas out. |
| `environment_ids` | Derived | Array of all `dbt_cloud_environment_id` values for this model. |

`int_snowflake__dbt_relation_history` has **one row per physical table**. A table built under several target names is one deployment: `target_name` is the latest build's target, and `target_names` and `dbt_cloud_environment_ids` list every one seen. Keeping one row per table keeps joins on the table name one-to-one, so recommendations aren't multiplied.

### Leaving out dev deployments

Many teams never clean up dev schemas, and seeing those costs can be useful, so by default nothing is excluded. Two variables leave dev deployments out of the recommendation backlog, the gold views and environment counts:

**`dbt_excluded_schemas`** (default `[]`): schema name patterns (`LIKE`, case-insensitive), e.g. `['DBT_%']` for personal dev schemas. Usually the most reliable option, since target names don't always separate dev from prod.

**`dbt_excluded_targets`** (default `[]`): target names, e.g. `['dev']`. A table is left out only when every target it was built under is excluded. A table built under both `dev` and `prod` stays.

Warehouse-level recommendations have no table, so they aren't affected.

### Project scoping

Two variables control what the gold layer surfaces:

**`dbt_monitored_projects`** (default: `[]` → resolves to current project):
- `[]` — only the project where the package is installed
- `['project_a', 'project_b']` — specific projects (mesh/multi-project)
- `['*']` — all dbt projects visible in the Snowflake account's QUERY_HISTORY

**`include_full_platform_insights`** (default: `false`):
- `false` — only entities connected to dbt queries from monitored projects
- `true` — include account-wide signals: all warehouses (even those no dbt project uses), CoCo/Intelligence spend, all users regardless of dbt involvement

The scope filter is applied once in `int_snowflake__all_recommendations` via the `scope_filter()` macro. All gold views inherit this filtering.

Warehouse-level config recommendations (idle credits, overload, etc.) have no direct model association. These are mapped to projects via the `wh_project` CTE in `int_snowflake__all_recommendations`, which identifies which project's models run on each warehouse by parsing dbt query comments. This means warehouse config recs automatically appear for any warehouse used by the monitored project(s).

AI services and other account-wide signals with no warehouse or project association only appear when `include_full_platform_insights = true`.

### Cross-environment monitoring

The package monitors all models visible in `QUERY_HISTORY` within the current Snowflake account. A single deploy can observe models from all environments (dev, staging, prod) as long as they share the same Snowflake account.

**Limitation:** For multi-account architectures (environments split across Snowflake accounts), deploy the package in each account independently. `QUERY_HISTORY` is account-scoped.

---

## 7. Limitations and Assumptions

### Cost estimation accuracy

| Domain | Accuracy | Why |
|--------|----------|-----|
| Warehouse sizing (idle credits) | High | Directly measured from metering |
| Expensive queries | High | QUERY_ATTRIBUTION_HISTORY provides exact per-query credits |
| Materialization | Medium | Assumes constant query volume; doesn't account for caching |
| Clustering | Medium | Uses cardinality-based scan reduction weighted by filter proportion; actual improvement depends on query patterns |
| Spillage | Low | Overhead-per-GB is estimated; actual impact varies by data types and operations |
| AI spend (model downgrade) | Medium | Assumes same quality of output from cheaper model |

### What we cannot estimate

- **Net savings after clustering maintenance cost** — reclustering credits depend on DML volume, which varies
- **Caching effects** — Snowflake's result cache may already reduce view recomputation; materializing may save less than projected
- **Concurrency impact** — scaling down a warehouse may increase queue time for concurrent queries
- **Quality impact of AI model downgrades** — cheaper models may produce worse results

### Assumptions

- `credit_rate_usd` is uniform across all services (may not be true for some contract types)
- Query patterns over the lookback window are representative of future patterns
- All recommendations are independent (in practice, fixing one may eliminate another)
- Cross-domain savings are NOT double-counted — the `combined_estimated_savings` in cross-domain insights represents the expected savings from addressing the root cause, not the sum of both domains

### Minimum savings threshold

Recommendations with `estimated_annual_savings_usd` below `min_annual_savings_usd` (default: $1) are downgraded to `backlog_status = 'stable'` in `int_snowflake__all_recommendations`. This filters noise (e.g., a view chain recommendation saving $0.0002/year) from all gold views without deleting the data. The threshold is configurable — organizations with higher spend may want to raise it to $10 or $100 to focus on material opportunities.
