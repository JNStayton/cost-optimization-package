# Materialization Recommendations — Redshift

This document covers the materialization optimization pipeline: identifying dbt models that should change materialization strategy (view to table, table to incremental) and recommending the optimal incremental configuration when applicable.

---

## Pipeline Overview

```
Phase 1: Candidate Identification
──────────────────────────────────

int_dbt__relations ──────────────────────┐
                                         ├──► fct_redshift__table_materialization_candidates
int_redshift__query_view_access ─────────┤      (views/ephemerals → table)
                                         │
int_redshift__view_chains ───────────────┘


int_dbt__relations ──────────────────────┐
                                         ├──► fct_redshift__incremental_materialization_candidates
int_redshift__table_query_stats_daily ───┤      (tables → incremental)
                                         │
int_redshift__table_inventory ───────────┘


Phase 2: Configuration (incremental only)
─────────────────────────────────────────

fct_redshift__incremental_              ┐
  materialization_candidates             ├──► fct_redshift__incremental_config_recommendations
                                         │      (strategy selection + key detection)
int_redshift__table_columns ─────────────┘
                                         │
                                    post-hook: probe_unique_key_candidates()
```

---

## Model 1: `fct_redshift__table_materialization_candidates`

### Purpose

Identifies dbt models materialized as `view` or `ephemeral` that are candidates for conversion to `table` materialization, based on query activity and view chain analysis.

### Attribution

Query and rebuild-cost attribution is based on the dbt manifest graph (`int_redshift__view_terminal_ancestors`), not Redshift's `pg_depend` catalogs or query-text matching. This works identically for late-binding views (`bind=false`, a common dbt-redshift production pattern) and regular views, since manifest-derived attribution has no dependency on how a view is bound in the warehouse.

A SELECT query is attributed to a view when it scans **all** of that view's terminal ancestors (its ultimate `source()` tables or `table`/`incremental` dbt models). Bytes and duration are summed only across the specific scan steps that hit those ancestors, not the whole query, so a view's attributed cost isn't inflated by unrelated tables scanned in the same query.

### Scoring

```
query_activity_score  = select_count * avg_gb_scanned_per_query * relative_duration_ratio
downstream_build_time_s = sum, across every downstream table this view feeds,
                           of that table's own average attributed rebuild cost
composite_chain_score = query_activity_score + downstream_build_time_s
```

Both terms are independent and additive. `downstream_build_time_s` already accounts for how many downstream tables a view feeds — each table's cost is summed individually rather than averaged and multiplied by a count — so no further multiplier is applied on top of it. `min_hops_to_table` is not part of the score; it's retained as an informational column only, since the cost of a view's position in a chain is already reflected in the measured build time, not in how many hops away the nearest table is.

### Decision Matrix

| Scenario | Recommendation |
|----------|---------------|
| `composite_chain_score >= table_materialization_min_composite_score` | Materialize as TABLE |
| Otherwise | Monitor |

`recommendation_confidence` is `high` when both `has_rebuild_cost_signal` and `has_query_activity_signal` are true, `medium` when only one is, `low` when neither is.

### Key Columns

| Column | Description |
|--------|-------------|
| `composite_chain_score` | `query_activity_score + downstream_build_time_s` |
| `rebuild_cost_score` | Alias for `downstream_build_time_s`, kept for readability |
| `is_in_view_chain` | Whether this view feeds into a downstream table |
| `min_hops_to_table` | Shortest path from this view to a materialized table (informational only) |
| `downstream_table_count` | Number of downstream tables that recompute this view |
| `recommendation_reason` | Natural language explanation of the recommendation |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `materialization_lookback_days` | `7` | Lookback window for query history |
| `table_materialization_min_query_count` | `10` | Minimum queries to appear in results |
| `table_materialization_min_composite_score` | `50` | Minimum score to recommend materialization |

---

## Model 2: `fct_redshift__incremental_materialization_candidates`

### Purpose

Identifies dbt models materialized as `table` that are candidates for conversion to `incremental` materialization, based on rebuild cost, table size, and rebuild redundancy. Excludes this package's own models (`package_name = 'dbt_cost_optimization_package'`) so the package never recommends converting its own intermediates.

### Scoring

**`compute_waste_score`** — primary ranking signal:
```
compute_waste_score = table_size_gb * builds_per_day
```
A large table rebuilt frequently wastes the most compute.

**`rebuild_redundancy_rate`** — efficiency signal:
```
rebuild_redundancy_rate = rows_at_period_start / rows_at_period_end
```
The fraction of each rebuild that reprocesses unchanged rows. A rate of 0.95 means 95% of every rebuild reproduces identical data.

**`est_daily_redundant_gb_scanned`** — impact estimate:
```
est_daily_redundant_gb_scanned = table_size_gb * builds_per_day * rebuild_redundancy_rate
```

### Trigger Logic

A table is surfaced when either:

| Trigger | Condition |
|---------|-----------|
| Build time + size | `max_build_time_sec >= incremental_candidates_min_build_time_sec` AND `table_size_gb >= incremental_candidates_min_size_gb` |
| Compute waste | `compute_waste_score >= incremental_candidates_min_compute_waste_score` AND `avg_build_time_sec >= incremental_candidates_min_compute_waste_avg_build_sec` |

### Recommendation Tiers

| Tier | Condition |
|------|-----------|
| Strong Candidate | `rebuild_redundancy_rate >= 0.9` |
| Candidate | `rebuild_redundancy_rate >= 0.7` |
| Candidate — Moderate Redundancy | `rebuild_redundancy_rate >= 0.5` |
| Low ROI — Minimal Rebuild Redundancy | `rebuild_redundancy_rate < 0.5` |
| Candidate — Insufficient History | Fewer than `incremental_candidates_min_qualified_build_days` CTAS build days recorded |
| Candidate — Verify Growth Signal | Row count decreased during the lookback window |

### Key Columns

| Column | Description |
|--------|-------------|
| `compute_waste_score` | `table_size_gb * builds_per_day` |
| `rebuild_redundancy_rate` | `rows_at_period_start / rows_at_period_end` |
| `est_daily_redundant_gb_scanned` | Estimated GB scanned redundantly per day |
| `growth_signal_reliable` | Whether enough qualified build history exists to trust the redundancy rate |
| `recommendation_reason` | Natural language explanation |

### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `incremental_candidates_lookback_days` | `60` | Lookback window for build history |
| `incremental_candidates_min_build_time_sec` | `300` | Min max build time for the build-time trigger |
| `incremental_candidates_min_size_gb` | `2` | Min table size for the size trigger |
| `incremental_candidates_min_compute_waste_score` | `5` | Min waste score for the waste trigger |
| `incremental_candidates_min_qualified_build_days` | `3` | Min CTAS build days to trust the growth signal |
| `incremental_candidates_min_compute_waste_avg_build_sec` | `30` | Min avg build time alongside the waste score |

---

## Model 3: `fct_redshift__incremental_config_recommendations`

### Purpose

For each table identified as an incremental candidate in Model 2, recommends:
- The optimal incremental strategy (`append`, `merge`, or `delete+insert` — Redshift has no `microbatch` strategy)
- The best filter column for `{% if is_incremental() %}` logic
- The most likely unique key column, confirmed via a cardinality probe
- A copy-pasteable dbt config template

This model has a `post_hook` (`probe_unique_key_candidates()`) that runs `APPROXIMATE COUNT(DISTINCT)` against candidate key columns to confirm uniqueness before recommending `merge` or `delete+insert`. When no single-column key is confirmed, the strategy is downgraded to `append` — a table's true unique key may be a composite of several columns together, which this probe cannot detect on its own.

### Deep-Dive Documentation

For the full strategy decision matrix, key column detection logic, and implementation steps, see:

**[Incremental Configuration Recommendations — Deep Dive](incremental_config_recommendations.md)**

### Key Outputs

| Column | Description |
|--------|-------------|
| `incremental_strategy` | Recommended strategy: `append`, `merge`, or `delete+insert` |
| `suggested_filter_column` | Best timestamp/date column for the incremental filter |
| `best_unique_key` | Top candidate by naming convention (unconfirmed) |
| `likely_unique_key` | Cardinality-confirmed unique key (populated by the post-hook) |
| `dbt_config_template` | Copy-pasteable dbt model config block |
| `strategy_notes` | Explanation of the strategy choice and next steps |

---

## Sample Queries

### Views that should be materialized

```sql
select
    table_fqn,
    model_name,
    materialized,
    composite_chain_score,
    downstream_table_count,
    select_count,
    avg_query_duration_s,
    recommendation,
    recommendation_reason
from <your_schema>.fct_redshift__table_materialization_candidates
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
from <your_schema>.fct_redshift__incremental_materialization_candidates
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
    dbt_config_template,
    strategy_notes
from <your_schema>.fct_redshift__incremental_config_recommendations
where likely_unique_key is not null
   or incremental_strategy = 'append'
order by est_daily_redundant_gb_scanned desc nulls last;
```

---

## Notes

- **Late-binding views:** `bind=false` views work with this package's attribution the same as regular views — manifest-graph attribution doesn't depend on `pg_depend`, which never populates for late-binding views at all.
- **Post-hook execution order:** `fct_redshift__incremental_config_recommendations` depends on its post-hook (`probe_unique_key_candidates()`) to populate `likely_unique_key`. On the first build, this column is null until the post-hook runs. A second build, or `dbt run --select fct_redshift__incremental_config_recommendations`, picks up the confirmed keys.
- **Strategy downgrade safety:** When no single-column unique key is confirmed, the model downgrades from `merge`/`delete+insert` to `append`, prioritizing data safety (visible duplicates) over silent data corruption from a non-unique key.
- **Composite keys:** if a table's real grain is a combination of columns (e.g. `order_id` + `line_number`), the naming-convention probe won't detect it — generate a surrogate key over the real grain columns with `dbt_utils.generate_surrogate_key()` and re-run to get a stronger recommendation.

---

## References

- [Redshift incremental models in dbt](https://docs.getdbt.com/docs/build/incremental-strategy#redshift)
- [SYS_QUERY_HISTORY](https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html)
- [SYS_QUERY_DETAIL](https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_DETAIL.html)
- [Redshift `WITH NO SCHEMA BINDING` (late-binding views)](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html)
