# Incremental Materialization Recommendations Mapping

Complete symptom-to-optimization map for incremental materialization recommendations. Covers candidate detection (Model 1), strategy selection (Model 2), and key column identification.

---

## Signal Availability

| Signal | Currently Measured? | Source |
|--------|:---:|--------|
| `rebuild_redundancy_rate` | Yes | Derived from first/last CTAS rows_inserted in lookback window |
| `builds_per_day` | Yes | COUNT of CTAS runs / days in window |
| `avg_build_time_sec` | Yes | AVG execution time of CTAS runs |
| `table_size_gb` | Yes | INT_SNOWFLAKE__TABLE_INVENTORY |
| `total_rows` | Yes | INT_SNOWFLAKE__TABLE_INVENTORY |
| `est_daily_redundant_gb_scanned` | Yes | size_gb * builds_per_day * redundancy_rate |
| `delete_count` | Yes | DML type = DELETE in QUERY_HISTORY (Enterprise) |
| `dml_count` | Yes | Total DML operations against the table |
| `suggested_filter_column` | Yes | INT_SNOWFLAKE__TABLE_COLUMNS (timestamp/date pattern matching) |
| `best_unique_key` | Yes | INT_SNOWFLAKE__TABLE_COLUMNS (naming convention: *_id, *_key, *_sk) |
| `likely_unique_key` (confirmed) | Yes | Post-hook probe_unique_key_candidates (APPROX_COUNT_DISTINCT >= 95%) |
| `has_external_deletes` | Yes | delete_count > 0 |
| `has_external_dml` | Yes | dml_count > 0 |
| `is_large_table` | Yes | rows > 10M OR size > 10 GB |
| `compute_waste_score` | Yes | table_size_gb * builds_per_day |
| Late-arriving record detection | No | Would need to compare max(filter_col) in source vs target over time |
| Source mutation pattern (CDC vs append) | No | Would need to analyze upstream query patterns or source config |
| Actual incremental savings (post-conversion) | No | Would need before/after comparison once model is converted |
| Query concurrency during builds | No | Available in QUERY_HISTORY but not correlated to specific models |
| Downstream impact of full-refresh failures | No | Would need DAG dependency depth analysis |

---

## Model 1: Candidate Detection (fct_snowflake__incremental_materialization_candidates)

### Entry Criteria (must meet at least one)

| # | Criterion | Threshold | Signal Source |
|---|---|---|---|
| E.1 | Max build time exceeds threshold | `max_build_time_sec >= 300` | Build history from query_history |
| E.2 | Table size exceeds threshold | `table_size_gb >= 2` | int_snowflake__table_inventory |
| E.3 | Compute waste score exceeds threshold (with min build time) | `compute_waste_score >= 5 AND avg_build_time_sec >= 30` | Derived: size_gb * builds_per_day |

### Recommendation Tiers

| # | Tier | Condition | Meaning | Action |
|---|---|---|---|---|
| T.1 | Strong Candidate | `rebuild_redundancy_rate >= 0.90` with reliable signal | 90%+ of each rebuild is redundant | High-confidence conversion target |
| T.2 | Candidate | `rebuild_redundancy_rate in [0.70, 0.90)` with reliable signal | Meaningful redundancy exists | Good conversion target |
| T.3 | Candidate -- Moderate Redundancy | `rebuild_redundancy_rate in [0.50, 0.70)` | Some overhead but growth is significant | Verify growth pattern first |
| T.4 | Low ROI -- Minimal Rebuild Redundancy | `rebuild_redundancy_rate < 0.50` | Table grows too quickly | Incremental won't save meaningfully; excluded from Model 2 |
| T.5 | Candidate -- Verify Growth Signal | Row count decreased mid-lookback | Possible full-refresh or upstream deletes | Investigate before converting |
| T.6 | Candidate -- Insufficient History | Fewer than `min_qualified_build_days` CTAS runs | Not enough data to trust the signal | Wait for more build history |

---

## Model 2: Strategy Selection (fct_snowflake__incremental_config_recommendations)

### Strategy Decision Tree (priority order, first match wins)

| # | Condition | Strategy | DDL/Config | Rationale |
|---|---|---|---|---|
| S.1 | `has_external_deletes AND has_filter_column` | `delete+insert` | `unique_key + incremental_predicates` | Scopes deletes to filter window; handles upstream delete propagation |
| S.2 | `has_external_deletes AND NOT has_filter_column` | `merge` | `unique_key` required | No time boundary; merge is only safe option for delete handling |
| S.3 | `has_unique_key AND has_filter_column AND is_large_table` | `delete+insert` | `unique_key + where filter_col > max(filter_col)` | Avoids full-target merge scan at large scale |
| S.4 | `has_unique_key AND has_filter_column AND NOT is_large_table` | `merge` | `unique_key + where filter_col > max(filter_col)` | Standard default; merge cost acceptable at moderate scale |
| S.5 | `has_filter_column AND NOT has_external_dml AND is_large_table` | `microbatch` | `event_time + batch_size` | Self-healing time batches; best for reliability at scale |
| S.6 | `has_filter_column AND NOT has_external_dml` | `append` | `where filter_col > max(filter_col)` | Simplest and cheapest; source is truly append-only |
| S.7 | `has_filter_column` (external DML, no key) | `append` | `where filter_col > max(filter_col)` | Safe default when key isn't available |
| S.8 | `has_unique_key AND NOT has_filter_column` | `merge` | `unique_key` only | No time boundary; merge required |
| S.9 | None of the above | `append` | No filter, no key | Safest default; flags key identification as next step |

### Post-Hook: Unique Key Verification (probe_unique_key_candidates)

| # | Probe Result | Action | Output |
|---|---|---|---|
| P.1 | Column confirmed (APPROX_COUNT_DISTINCT >= 95% of rows) | Keep strategy as-is | `identified_unique_key` populated, `dbt_model_config` references confirmed key |
| P.2 | No single-column key confirmed AND strategy was merge/delete+insert | **Downgrade to append** | Strategy overwritten; `strategy_notes` explains surrogate key path |
| P.3 | No single-column key confirmed AND strategy was already append | No change | Key identification flagged as TODO in config template |

---

## Coverage Assessment

### What's well-covered (high confidence)

| Area | Status | Notes |
|------|--------|-------|
| Rebuild redundancy detection | Strong | First/last CTAS row comparison is reliable for table-materialized models |
| Strategy selection matrix | Strong | All 5 Snowflake strategies covered with clear priority order |
| Filter column detection | Strong | Comprehensive pattern matching with ranked priority |
| Unique key detection + verification | Strong | Naming convention + cardinality probe + safe downgrade |
| Config template generation | Strong | Copy-pasteable with all parameters pre-filled |
| Large table branching (merge vs delete+insert) | Strong | Size-aware, correct threshold logic |

### What could be refined/tightened

| Area | Current State | Refinement Opportunity |
|------|---|---|
| Redundancy rate calculation | Uses first/last CTAS rows_inserted | Could use median or percentile to be more robust against outlier runs (one unusually small run skews the rate) |
| "Strong Candidate" labeling passthrough | Tier name still appears in some recommendation contexts | Already fixed in gold layer — config rec reason now shows redundancy % directly |
| Microbatch recommendation | Only triggers for large + no DML + filter column | Could be recommended more broadly — microbatch is safer than append for any event-time data regardless of size (self-healing property) |
| Delete detection | Uses `delete_count > 0` from query_history | Could distinguish between scheduled full-refresh deletes (dbt's own DELETE before CTAS) and true external deletes. Currently may false-positive on full-refresh patterns. |
| Unique key probe downgrade messaging | `strategy_notes` explains two paths | Could be more prescriptive — if table has a natural grain visible in column combinations, suggest the specific surrogate key columns |
| `insert_overwrite` strategy | Listed in docs but never recommended | Not implemented in the decision tree — should be added for tables with confirmed clustering keys and clean partition boundaries |

### Gaps in coverage

| Gap | Impact | Difficulty to Implement |
|-----|--------|------------------------|
| **Late-arriving data detection** | High — append/microbatch strategies silently miss late records. If late data is common, merge with lookback window is needed. | Medium — would need to track max(filter_col) over time and detect if older records appear after expected completion |
| **Source mutation pattern classification** | Medium — knowing if upstream is CDC, full-load, or append-only would inform strategy confidence | High — requires understanding the upstream source (EL tool config, source freshness patterns) |
| **Post-conversion validation** | Medium — no feedback loop to confirm the recommendation worked | Medium — could compare build time and credits before/after conversion (requires tracking the event) |
| **Multi-column key detection** | Medium — fact tables often need composite keys that naming conventions don't catch | Medium — could analyze GROUP BY patterns in downstream queries to infer grain |
| **Partition-aligned insert_overwrite** | Low-Medium — useful for date-partitioned large tables | Medium — needs clustering key awareness + partition boundary analysis |
| **Incremental model health monitoring** | Low (post-conversion concern) | Low — track row growth rate, late-arriving %, full-refresh frequency for models already converted |
| **Cost impact estimation accuracy** | Low — current estimate uses avg_build_time * redundancy_rate | Low — could use QUERY_ATTRIBUTION_HISTORY for exact per-build credits |
| **Downstream freshness impact** | Low — converting to incremental can increase data freshness for downstream | Low — could note "downstream models will see data X minutes sooner" |

---

## Relationship to Other Domains

| Cross-Domain Signal | How It Connects | Current State |
|---|---|---|
| Expensive query (same model) | An expensive query that rebuilds a full table → incremental would reduce cost | Detected in cross_domain_insights (signal: incremental + expensive_query) |
| Spillage (same model) | Full rebuild overflows memory → incremental reduces working set | Detected in cross_domain_insights (signal: spillage + incremental) |
| Clustering candidate (same table) | Table needs clustering AND incremental → do incremental first (reduces rebuild, makes clustering maintenance cheaper) | Detected in cross_domain_insights hierarchy |
| View chain (upstream) | A view upstream of this table causes cascading recomputation → materialize the view first | Detected via materialization v2 candidates |
| Warehouse idle credits | Infrequent but expensive builds keep warehouse alive → incremental reduces build time → warehouse can suspend sooner | Not explicitly linked yet (backlog: expensive query cost growth mapping) |

---

## Size Threshold Reference

| Variable | Default | Effect |
|---|---|---|
| `incremental_large_table_row_threshold` | 10,000,000 | Above this: prefer delete+insert over merge (avoids full-target scan) |
| `incremental_large_table_gb_threshold` | 10 GB | Above this: prefer delete+insert over merge |
| `incremental_unique_key_probe_threshold` | 0.95 | APPROX_COUNT_DISTINCT / COUNT(*) must exceed this to confirm a key |
| `incremental_candidates_min_build_time_sec` | 300 (5 min) | Minimum build duration to trigger candidate consideration |
| `incremental_candidates_min_size_gb` | 2 GB | Minimum table size to trigger candidate consideration |
| `incremental_candidates_min_compute_waste_score` | 5 | size_gb * builds_per_day minimum |
| `incremental_candidates_min_qualified_build_days` | 3 | Minimum CTAS runs needed to trust redundancy signal |
