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
| **Agents** (Cortex Agents, CI automation, ticket bots) | Structured fields, self-contained rows, node_id for code navigation | SQL queries against gold views, actionable_sql field for auto-apply |

---

## 2. Views

### `vw_snowflake__top_recommendations`

**The flagship view.** All domains, ranked by estimated savings. One row per actionable recommendation, deduplicated by node_id where applicable.

| Column | Type | Description |
|--------|------|-------------|
| `priority_rank` | int | Dense rank by `estimated_annual_savings_usd` DESC |
| `domain` | string | warehouse / materialization / clustering / ai |
| `effort_category` | string | config_change / sql_refactor / architecture |
| `node_id` | string | Logical dbt model identifier (nullable) |
| `model_name` | string | Human-readable model name |
| `table_fqn` | string | Physical relation |
| `warehouse_name` | string | Relevant warehouse (for warehouse domain recs) |
| `recommendation` | string | Short action text |
| `recommendation_reason` | string | Detailed evidence with metrics |
| `estimated_annual_cost_usd` | float | "If unchanged, expect this yearly cost" |
| `estimated_annual_savings_usd` | float | "If fixed, save this amount yearly" |
| `score` | float | Domain-specific impact score (not normalized across domains) |
| `actionable_sql` | string | DDL/config snippet to apply the fix |
| `snapshot_date` | date | When this analysis was produced |

Filters out "Monitor" and "Stable" recommendations. Only actionable items appear.

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

**Sprint planning / agent intake view.** Every recommendation with full context.

Same columns as `top_recommendations` plus:
- Includes ALL tiers (actionable + "Monitor" + "Stable")
- `backlog_status`: actionable / monitor / stable
- `dbt_config_template`: from incremental config recs (the ready-to-paste config block)
- `validate_uniqueness_sql`: from incremental config recs (the test query)

Ordered by: `effort_category ASC` (quick wins first), then `estimated_annual_savings_usd DESC`.

An agent should be able to pick any row from this view and:
- For `config_change`: auto-generate a PR
- For `sql_refactor`: investigate the model SQL and propose changes
- For `architecture`: create a ticket with all evidence attached

---

### `vw_snowflake__recommendations_by_environment`

**Environment drill-down.** Does NOT collapse by node_id — every physical FQN gets its own row.

Same columns as `top_recommendations` plus:
- `target_name`: which dbt target (dev / prod / default)
- `environment_count`: how many envs this model exists in
- `has_prod_relation`: true if the model exists in a prod-like env
- `has_nonprod_only`: true if the model ONLY exists in non-prod (flag for attention)

Use cases:
- `WHERE target_name LIKE '%prod%'` — "show me only prod recommendations"
- `WHERE has_nonprod_only = true` — "what needs fixing before it reaches prod?"
- Compare same model across environments — "is dev worse than prod?"

---

### `vw_snowflake__cross_domain_insights`

**Multi-signal correlation view.** Surfaces cases where recommendations from different domains point to the same root cause or compound each other.

| Column | Type | Description |
|--------|------|-------------|
| `insight_id` | string | Unique identifier for this insight |
| `insight_type` | string | The correlation pattern detected |
| `table_fqn` | string | The table/model at the center of the correlation |
| `node_id` | string | Logical dbt model (nullable) |
| `model_name` | string | Human-readable name |
| `domains_involved` | array | Which domains overlap (e.g., ['warehouse', 'clustering']) |
| `root_cause` | string | What we believe is the underlying issue |
| `recommended_fix_order` | string | Which domain to fix first |
| `combined_estimated_savings_usd` | float | Projected savings if the root cause is addressed |
| `evidence` | string | Detailed metrics from each domain |

See Section 5 for all correlation rules.

---

### `vw_snowflake__expensive_users`

**User-level cost attribution.** Aggregates credits consumed by user across expensive queries and AI usage.

| Column | Type | Description |
|--------|------|-------------|
| `user_name` | string | Snowflake user |
| `role_name` | string | Primary role (default_role for AI, top_role for queries) |
| `query_credits_30d` | float | Credits from expensive queries attributed to this user |
| `ai_credits_30d` | float | Credits from AI/Cortex usage |
| `combined_credits_30d` | float | Total credits across all attributable domains |
| `estimated_annual_cost_usd` | float | Projected annual cost |
| `top_expensive_query_hash` | string | Their most expensive recurring query |
| `top_ai_model` | string | Their most-used AI model |
| `query_count_30d` | int | Total queries attributed |
| `recommendation` | string | Action/awareness text |

---

## 3. Cost Estimation Methodology

### Credits-per-second derivation

Snowflake bills warehouse compute by the hour at fixed rates per size:

| Size | Credits/Hour | Credits/Second |
|------|-------------|----------------|
| X-Small | 1 | 0.000278 |
| Small | 2 | 0.000556 |
| Medium | 4 | 0.001111 |
| Large | 8 | 0.002222 |
| X-Large | 16 | 0.004444 |
| 2X-Large | 32 | 0.008889 |
| 3X-Large | 64 | 0.017778 |
| 4X-Large | 128 | 0.035556 |

Rather than hardcoding these rates, we derive the actual rate per warehouse from `int_snowflake__warehouse_daily`:

```sql
credits_per_second = avg(credits_used_compute) / 3600.0
```

This handles multi-cluster warehouses (which can consume more than the base rate) and warehouses that aren't active for the full hour.

### Dollar conversion

All dollar estimates use the configurable `credit_rate_usd` variable (default: $2/credit). Users should set this to their contract rate.

```
estimated_annual_cost_usd = annual_credits × credit_rate_usd
```

### Per-domain cost estimation

#### Warehouse sizing

| Metric | Formula |
|--------|---------|
| Current annual cost | `total_credits_30d × 12 × credit_rate_usd` |
| Savings (scale down) | `total_idle_credits_30d × 12 × credit_rate_usd` |
| Savings (MCW recommendation) | Not directly estimable — flagged as potential improvement |
| Savings (Gen2) | Estimated 10-20% on DML-heavy workloads (conservative: 10%) |

#### Expensive queries

| Metric | Formula |
|--------|---------|
| Current annual cost | `estimated_annual_cost_usd` (already computed in fact model) |
| Savings | Conservative 20% reduction estimate (refactoring typically achieves 20-80%) |

#### Materialization (view → table)

| Metric | Formula |
|--------|---------|
| Current annual cost | `select_count × avg_query_duration_s × credits_per_second × 365 × credit_rate_usd` |
| Savings | Same minus the cost of 1 daily table build: `(select_count - 1) × avg_duration × credits_per_sec × 365 × credit_rate_usd` |

Logic: A view recomputes on every SELECT. A table computes once and is read cheaply. The savings is the eliminated recomputation cost.

#### Spillage

| Metric | Formula |
|--------|---------|
| Current annual cost | Attributed from the query's total credit cost (already in expensive_queries) |
| Savings | Estimated time overhead from spilling: `total_gb_spilled × 0.5 seconds_per_gb_spilled × credits_per_second × 365 × credit_rate_usd` |

The `0.5 seconds per GB spilled` is a conservative estimate. Local spillage adds ~0.5-2 seconds per GB; remote spillage adds ~2-10 seconds per GB.

#### Clustering

| Metric | Formula |
|--------|---------|
| Current annual cost | `select_count × avg_query_duration_s × credits_per_second × 365 × credit_rate_usd` |
| Savings | `current_cost × (1 - target_scan_ratio / current_scan_ratio)` |

Assumes clustering would reduce scan_ratio to ~0.2 (well-clustered target). If current scan_ratio is 1.0 (no pruning), savings = 80% of current query cost.

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
| Incremental config | Strategy recommended (with template) | The `dbt_config_template` column has the ready-to-paste block |
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

**Detection:** Same `table_fqn` appears in `fct_table_materialization_candidates_v2` (recommendation = 'Materialize as TABLE') AND a downstream table in `fct_warehouse_spillage_recommendations`.

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

**Detection:** A `table_fqn` in `fct_table_materialization_candidates_v2` has a downstream table whose build queries appear in `fct_expensive_query_recommendations`.

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

**Detection:** A `table_fqn` in `fct_table_materialization_candidates_v2` is upstream (in `int_dbt__relations.parent_models`) of a `table_fqn` in `fct_incremental_materialization_candidates`.

**Root cause:** A view feeds into a table that should be incremental. Both changes together compound: materializing the view reduces the incremental model's scan cost, and making it incremental reduces redundant rebuilds.

**Recommended fix order:** Materialize the view first (quick config change), then convert the downstream to incremental (architecture work).

**Evidence:** "View {view_model} feeds table {table_model}. Materializing the view AND converting to incremental would compound savings: eliminated recomputation + eliminated redundant rebuilds."

---

### Correlation 12: Package Self-Referential Spillage

**Detection:** `fct_warehouse_spillage_recommendations` contains rows where `package_name = 'dbt_cost_optimization_package'`.

**Root cause:** The optimization package's own models are spilling because they query large ACCOUNT_USAGE views. This is a "heal thyself" signal.

**Recommended fix order:** Scale up the build warehouse for package model runs, or increase the warehouse's auto-suspend timeout to avoid cold starts.

**Evidence:** "Package model {model_name} spills {gb_spilled} GB on warehouse {warehouse_name}. Consider using a larger warehouse for the cost optimization job, or materializing upstream intermediates."

---

## 6. Environment Priority

When multiple environments have the same recommendation for the same logical model, the gold layer deduplicates using this priority:

| Priority | Target Names Matched |
|----------|---------------------|
| 1 (highest) | `prod`, `default`, `production`, `main`, or any name containing `prod` |
| 2 | Any name containing `stag` |
| 3 (lowest) | Everything else (dev, CI, feature branches) |

The "representative" row is the one from the highest-priority environment. All other environment FQNs are preserved in the `all_table_fqns` array for drill-down.

**Dedup rule:** If the same model has DIFFERENT recommendations across environments (e.g., "Materialize as TABLE" in dev, "Monitor" in prod), both appear as separate rows. They represent genuinely different signals.

If the same model has the SAME recommendation across environments, only the highest-priority environment's row is kept.

---

## 7. Limitations and Assumptions

### Cost estimation accuracy

| Domain | Accuracy | Why |
|--------|----------|-----|
| Warehouse sizing (idle credits) | High | Directly measured from metering |
| Expensive queries | High | QUERY_ATTRIBUTION_HISTORY provides exact per-query credits |
| Materialization | Medium | Assumes constant query volume; doesn't account for caching |
| Clustering | Medium-Low | Assumes target scan_ratio of 0.2; actual improvement depends on key choice and query patterns |
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
