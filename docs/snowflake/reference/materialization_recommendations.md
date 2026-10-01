# Materialization Recommendations — Snowflake

This document covers the materialization optimization pipeline: identifying dbt models that should change materialization strategy (view to table, table to incremental) and recommending the optimal incremental configuration when applicable.

---

## Pipeline Overview

The materialization pipeline is a two-phase flow:

```
Phase 1: Candidate Identification
──────────────────────────────────

int_dbt__relations ──────────────────────┐
                                         ├──► fct_snowflake__table_materialization_candidates
int_snowflake__query_history ────────────┤      (views/ephemerals → table)
                                         │
int_snowflake__view_chains ──────────────┘


int_dbt__relations ──────────────────────┐
                                         ├──► fct_snowflake__incremental_materialization_candidates
int_snowflake__table_query_stats_daily ──┤      (tables → incremental)
                                         │
int_snowflake__table_inventory ──────────┘


Phase 2: Configuration (incremental only)
─────────────────────────────────────────

fct_snowflake__incremental_              ┐
  materialization_candidates             ├──► fct_snowflake__incremental_config_recommendations
                                         │      (strategy selection + key detection)
int_snowflake__table_columns ────────────┘
                                         │
                                    post-hook: probe_unique_key_candidates()
```

---

## Model 1: `fct_snowflake__table_materialization_candidates`

### Purpose

Identifies dbt models materialized as `view` that are candidates for conversion to `table` materialization, based on query volume, data scan cost, and view chain analysis. Ephemerals are never candidates (there's no relation to read, build, or probe), but they're part of the chains: a view above an ephemeral is credited with the builds of the table below it.

### Scoring

Two scoring signals are computed:

**`materialization_score`** — query-centric signal:
```
materialization_score = select_count * avg_query_duration_s
```
Direct measure of total time spent recomputing the view.

**`composite_chain_score`** — chain-aware signal (higher priority):
```
composite_chain_score = (
    max(select_count, 1) * avg_gb_scanned_per_query * relative_duration_ratio
    + downstream_build_time_s
) * min_hops_to_table * max(downstream_table_count, 1)
```

The chain score accounts for cascading recomputation: a view that is 3 hops from the nearest materialized table and has 5 downstream tables causes 5x the recomputation at 3x the depth.

### Decision Matrix

| Scenario | Recommendation | Rationale |
|----------|---------------|-----------|
| View is in a chain AND `composite_chain_score > 0` | Materialize as TABLE | Cascading recomputation is expensive; materializing eliminates redundant work for all downstream consumers |
| Not in chain AND `materialization_score > 500` AND `total_gb_scanned > 10` | Materialize as TABLE | High query volume with large data scan — repeated view computation is expensive |
| Not in chain AND `avg_query_duration_s > 10` AND `select_count > 50` | Materialize as TABLE | Slow average query time on a frequently queried view |
| Otherwise | Monitor | Query volume or execution time below recommendation thresholds |

### One view per chain

The views in a chain are alternatives: materializing any one of them removes the recompute that every build of the downstream table pays. So for each table at the end of a chain, the view with the highest `net_recompute_s_saved` is recommended (ties go to the view nearest the table, which covers the most upstream work), and the others are alternatives with status `monitor`.

```
net_recompute_s_saved = max(select_count + downstream_build_count - view_build_runs, 0) × recompute_cost_s
```

`recompute_cost_s` comes from the view probe when there is one: a post-hook on `int_snowflake__view_probe` runs `select hash_agg(*) from <view>` with the result cache off (`hash_agg(*)` forces every column to be computed; `count(*)` would let Snowflake skip the column work). Without a probe it falls back to the view's average read duration.

### Key Columns

| Column | Description |
|--------|-------------|
| `materialization_score` | `select_count * avg_duration_s` — total recomputation time |
| `composite_chain_score` | Chain-aware score incorporating downstream build time and depth |
| `is_in_view_chain` | Whether this view feeds into other views before reaching a table |
| `min_hops_to_table` | Shortest path (in ref hops) from this view to a materialized table |
| `downstream_table_count` | Number of downstream tables that recompute this view |
| `downstream_build_count` | Builds of every downstream table in the lookback window, each recomputing this view |
| `recompute_cost_s` / `recompute_cost_source` | Seconds per run of the view's query, and whether it was measured by the probe (`probe`) or taken from reads (`reads`) |
| `net_recompute_s_saved` | Seconds of recompute removed per lookback window by materializing |
| `chain_role` | `recommended`, `alternative`, or `standalone` |
| `chosen_view_for_chain` | The recommended view in this view's chain |
| `recommendation_reason` | Natural language explanation of the recommendation |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `table_materialization_lookback_days` | `14` | Lookback window for query history |
| `table_materialization_min_query_count` | `10` | Minimum queries to appear in results |
| `table_materialization_view_probe_limit` | `10` | Most views probed per run (deepest chains first); `0` turns the probe off |
| `table_materialization_view_probe_refresh_days` | `7` | Re-probe a view after this many days |

The probe skips this package's own views, views outside `dbt_monitored_projects`, and schemas in `dbt_excluded_schemas`. A probe that fails (a dropped view, or no SELECT privilege) is recorded as failed instead of failing the build, and that view falls back to its read duration. The probe time is measured on the warehouse running the package.

---

## Model 2: `fct_snowflake__incremental_materialization_candidates`

### Purpose

Identifies dbt models materialized as `table` that are candidates for conversion to `incremental` materialization, based on rebuild cost, table size, and rebuild redundancy.

### Scoring

**`compute_waste_score`** — primary ranking signal:
```
compute_waste_score = table_size_gb * builds_per_day
```
A large table rebuilt frequently wastes the most compute. Simple, intuitive, and directly proportional to credit waste.

**`rebuild_redundancy_rate`** — efficiency signal:
```
rebuild_redundancy_rate = median(prev_day_rows / current_day_rows)
                          across consecutive build days in the lookback window
```
Measures the median fraction of each rebuild that reprocesses unchanged rows, computed from consecutive-day row count deltas using LAG. A rate of 0.95 means the typical build reprocesses 95% unchanged data — only 5% was new, but the entire table was rebuilt. Higher = more waste. This approach is resilient to one-time data reloads (outlier pairs produce NULL and are excluded from the median).

**`est_daily_redundant_gb_scanned`** — dollar impact:
```
est_daily_redundant_gb_scanned = table_size_gb * builds_per_day * rebuild_redundancy_rate
```
Estimates how many GB are scanned redundantly per day due to full rebuilds.

### Trigger Logic

A table appears in recommendations when ANY of these conditions is met:

| Trigger | Condition | Rationale |
|---------|-----------|-----------|
| **Compute waste** | `compute_waste_score >= 5` AND `avg_build_time_sec >= 30` | Large table rebuilt often with non-trivial build time |
| **Build time + size** | `max_build_time_sec >= 300` AND `table_size_gb >= 2` | Single builds are slow and the table is large enough to benefit |

### Recommendation Tiers

| Tier | Condition | Meaning |
|------|-----------|---------|
| Strong Candidate | `redundancy_rate >= 0.9` | 90%+ of data is unchanged between rebuilds |
| Candidate | `redundancy_rate >= 0.7` | 70-90% unchanged — clear benefit |
| Candidate — Moderate Redundancy | `redundancy_rate >= 0.5` | 50-70% unchanged — benefit exists but smaller |
| Low ROI — Minimal Rebuild Redundancy | `redundancy_rate < 0.5` | Most data changes between rebuilds — incremental may not help much |
| Candidate — Insufficient History | `qualified_build_pairs < min_qualified_build_days - 1` | Not enough consecutive build days to compute per-build redundancy |
| Candidate — Verify Growth Signal | Insufficient consecutive pairs with valid deltas | Growth pattern unclear; verify data lifecycle |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `incremental_candidates_lookback_days` | `60` | Lookback window for build history |
| `incremental_candidates_min_build_time_sec` | `300` | Min max build time for build-time trigger |
| `incremental_candidates_min_size_gb` | `2` | Min size for size trigger |
| `incremental_candidates_min_compute_waste_score` | `5` | Min waste score for waste trigger |
| `incremental_candidates_min_qualified_build_days` | `3` | Min CTAS days to trust growth signal |
| `incremental_candidates_min_compute_waste_avg_build_sec` | `30` | Min avg build time alongside waste score |
| `incremental_candidates_roi_high_build_time_sec` | `300` | Min avg build time (seconds) for 'high' ROI tier |
| `incremental_candidates_roi_medium_build_time_sec` | `120` | Min avg build time (seconds) for 'medium' ROI tier |

---

## Model 3: `fct_snowflake__incremental_config_recommendations`

### Purpose

For each table identified as an incremental candidate in Model 2, recommends:
- The optimal incremental strategy (append, merge, delete+insert, microbatch)
- The best filter column for `{% if is_incremental() %}` logic
- The most likely unique key column (confirmed via cardinality probe)
- A copy-pasteable dbt config template

This model has a `post_hook` (`probe_unique_key_candidates()`) that runs `APPROX_COUNT_DISTINCT` against candidate key columns to confirm uniqueness before recommending `merge` or `delete+insert` strategies.

### Deep-Dive Documentation

For the full strategy decision matrix, key column detection logic, implementation steps, and Snowflake strategy performance guide, see:

**[Incremental Configuration Recommendations — Deep Dive](incremental_config_recommendations.md)**

### Key Outputs

| Column | Description |
|--------|-------------|
| `incremental_strategy` | Recommended strategy: `append`, `merge`, `delete+insert`, or `microbatch` |
| `suggested_filter_column` | Best timestamp/date column for incremental filter |
| `best_unique_key` | Top candidate by naming convention (unconfirmed) |
| `likely_unique_key` | Cardinality-confirmed unique key (populated by post-hook) |
| `dbt_model_config` | Copy-pasteable dbt model config block |
| `identified_unique_key` | Confirmed unique key column name (only populated for merge/delete+insert strategies; NULL for append) |
| `strategy_notes` | Explanation of strategy choice and next steps |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `incremental_large_table_row_threshold` | `10000000` | Row count above which `delete+insert` preferred over `merge` |
| `incremental_large_table_gb_threshold` | `10` | Size above which `delete+insert` preferred over `merge` |
| `incremental_unique_key_probe_threshold` | `0.95` | `APPROX_COUNT_DISTINCT` / `COUNT(*)` ratio required to confirm a column as a likely unique key. Below this threshold, the strategy is downgraded to `append`. |
| `incremental_unique_key_probe_threshold` | `0.95` | APPROX_COUNT_DISTINCT / COUNT ratio to confirm uniqueness |

---

## Sample Queries

### Views that should be materialized (chain analysis)

```sql
select
    table_fqn,
    model_name,
    materialized,
    composite_chain_score,
    min_hops_to_table,
    downstream_table_count,
    select_count,
    avg_query_duration_s,
    recommendation,
    recommendation_reason
from <your_schema>.fct_snowflake__table_materialization_candidates
where recommendation = 'Materialize as TABLE'
order by composite_chain_score desc;
```

### Top incremental candidates by wasted compute

```sql
select
    table_fqn,
    model_name,
    table_size_gb,
    builds_per_day,
    avg_build_time_sec,
    rebuild_redundancy_rate,
    compute_waste_score,
    est_daily_redundant_gb_scanned,
    recommendation
from <your_schema>.fct_snowflake__incremental_materialization_candidates
order by est_daily_redundant_gb_scanned desc nulls last;
```

### Ready-to-implement incremental configs

```sql
select
    table_fqn,
    model_name,
    incremental_strategy,
    suggested_filter_column,
    likely_unique_key,
    dbt_model_config,
    strategy_notes
from <your_schema>.fct_snowflake__incremental_config_recommendations
where likely_unique_key is not null
   or incremental_strategy = 'append'
order by est_daily_redundant_gb_scanned desc nulls last;
```

---

## Notes

- **ILIKE matching:** `fct_snowflake__table_materialization_candidates` uses `query_text ILIKE '%view_name%'` for view-level attribution. This can produce false positives for views with short or common names. ACCESS_HISTORY cannot be used for views because it resolves views to their base tables.
- **Post-hook execution order:** `fct_snowflake__incremental_config_recommendations` depends on its post-hook (`probe_unique_key_candidates()`) to populate `likely_unique_key`. On the first build, this column is null until the post-hook runs. A second build or a `dbt run --select fct_snowflake__incremental_config_recommendations` will pick up the confirmed keys.
- **Strategy downgrade safety:** When no single-column unique key is confirmed, the model downgrades from `merge`/`delete+insert` to `append`. This prioritizes data safety (visible duplicates) over silent data corruption from non-unique keys.
- **Edition requirements:** All three models work on both Standard and Enterprise editions. Enterprise provides better query-to-table attribution via ACCESS_HISTORY in the upstream `int_snowflake__table_query_stats_daily`.

---

## References

- [Snowflake incremental models in dbt](https://docs.getdbt.com/docs/build/incremental-strategy#snowflake)
- [dbt microbatch strategy](https://docs.getdbt.com/docs/build/incremental-strategy#microbatch)
- [Snowflake MERGE performance](https://docs.snowflake.com/en/sql-reference/sql/merge)
- [Automatic Clustering](https://docs.snowflake.com/en/user-guide/tables-auto-reclustering)
