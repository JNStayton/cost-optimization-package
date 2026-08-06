# Incremental configuration recommendations — Snowflake

## Overview

`fct_snowflake__incremental_config_recommendations` identifies dbt `table`-materialized models that are candidates for conversion to `incremental` materialization, infers the optimal strategy from data semantics, and produces a confidence-scored recommendation with a copy-pasteable dbt config template.

This model depends on `fct_snowflake__incremental_materialization_candidates`, which handles candidate identification, ROI gating, and rebuild pressure scoring.

For the full confidence system design, see [incremental_recommendations_mapping.md](incremental_recommendations_mapping.md).

---

## Strategy decision matrix

Strategies are selected by **data-change semantics** inferred from telemetry — not table size. The package evaluates available signals (watermark columns, unique keys, target DML patterns) to determine the most appropriate strategy.

| # | Condition (inferred from telemetry) | Recommended strategy | Assumptions made |
|---|---|---|---|
| S.1 | Has proposed watermark + exact non-null key + no target deletes/updates | `merge` | "Watermark is change boundary", "data is time-local", "lookback captures late arrivals" |
| S.2 | Has proposed watermark + INSERT-only target DML + no key needed (or key confirmed) | `append` | "Source is append-only", "no late arrivals beyond lookback", "no upstream mutations" |
| S.3 | Has proposed watermark + exact key + target deletes observed | `merge` (with delete warning) | Same as S.1 + "deletes will NOT propagate — target retains deleted rows" |
| S.4 | Has proposed watermark + no key candidate | `append` (weaker confidence) | "Source is append-only", "duplicates won't occur", "no key needed" |
| S.5 | No proposed watermark candidate | Cannot propose strategy | No template possible — `investigate` or `do_not_recommend` |

### Phase 1 actionable strategies

Only `merge` and `append` can reach `actionable_review` status in Phase 1. Other strategies remain `investigate`:

| Strategy | Phase 1 status | Phase 2 potential |
|----------|---------------|-------------------|
| `merge` | `actionable_review` (confidence >= 60) | Same |
| `append` | `actionable_review` (confidence >= 60) | Same |
| `delete+insert` | `investigate` (always) | Upgradeable with validated slice selectivity |
| `microbatch` | `investigate` (always) | Upgradeable with event-time validation |

---

## Snowflake strategy performance guide

Snowflake's incremental strategies differ significantly in cost. Prefer strategies that scope work to a bounded window rather than scanning the full target table.

### `append`
Inserts new rows only. No matching against existing rows. The cheapest strategy and the right choice when the source is truly append-only (no late-arriving updates or deletes).

### `merge`
Uses a Snowflake `MERGE` statement to match on `unique_key` and update or insert. Requires a validated unique key. At large scale the match scan reads the full target table — but the package recommends merge based on data semantics (key + watermark + no deletes), not table size.

### `delete+insert`
Deletes matching rows from the target scoped to a filter window, then inserts new rows. More efficient than `merge` for large tables because the delete is bounded. Requires both `unique_key` and a filter column. Currently `investigate` status only — the package cannot validate slice recomputability from telemetry alone.

### `microbatch` *(dbt Core 1.9+)*
Processes data in configurable time-based batches. Self-healing: failed batches can be retried independently. Currently `investigate` status only — requires validation of batch independence and parent event-time propagation.

**Cost ranking (cheapest to most expensive):** `append` < `microbatch` < `delete+insert` < `merge`

---

## Confidence scoring

Every candidate that passes the ROI gate receives a confidence score (0-100) that determines its recommendation status:

| Confidence range | Status | Template? | Gold view |
|-----------------|--------|-----------|-----------|
| >= 60 | `actionable_review` | Yes | Model optimizations |
| 30-59 | `investigate` | Yes (with verify warnings) | Optimization backlog |
| 1-29 | `investigate` | Yes (if constructible) | Optimization backlog |
| NULL (no strategy) | `do_not_recommend` | No | Excluded |

The score starts at 100 and is adjusted by deductions (for assumptions and blocking signals) and bonuses (for validated evidence). See [incremental_recommendations_mapping.md](incremental_recommendations_mapping.md) for the full scoring model.

---

## Key column detection

### Filter column (`suggested_filter_column`)

Identifies the best timestamp or date column for the incremental filter predicate.

Detection order:
1. Restrict to columns with timestamp or date data type
2. Rank by column name pattern:
   - `*updated_at*`, `*modified_at*` — highest priority (captures late-arriving changes)
   - `*loaded_at*`, `*ingested_at*`, `*inserted_at*`, `*synced_at*` — ingestion time
   - `*created_at*`, `*event_date*`, `*event_time*`, `*event_timestamp*` — creation/event time
   - Any other timestamp/date — lowest priority

### Unique key validation

The `probe_unique_key_candidates` post-hook validates key candidates using an **exact probe**:

```sql
count(*) = count(distinct <key_column>)
AND count_if(<key_column> IS NULL) = 0
```

This is an exact uniqueness check (not approximate). A key passes only if it is perfectly unique with zero nulls.

| Probe result | Effect on confidence | Effect on strategy |
|---|---|---|
| **Pass** (exact unique + zero nulls) | +10 (restores the naming-convention deduction) | Enables merge strategy |
| **Fail** (duplicates or nulls) | -30, adds `key_not_exact_or_nullable` blocking signal | Clears `identified_unique_key`, forces append |

**`identified_unique_key`** is only populated when the recommended strategy is `merge` AND the key probe passes. For `append` strategies, this is always NULL — append doesn't use a unique key.

---

## Implementing the recommendation

1. Review `confidence_score` and `assumptions` array — understand what the package inferred
2. Check `blocking_signals` — these are the specific risks to verify
3. Copy the `dbt_model_config` into your model file
4. If merge strategy: verify the unique key is truly your table's grain (the probe validates uniqueness on the current target, not semantic correctness)
5. Add a `unique` test to your schema YAML for the key column
6. Run a full-refresh on the first incremental run: `dbt run --full-refresh --select <model>`
7. Monitor for duplicates (append) or missed mutations (merge) over a few build cycles

---

## Variables

| Variable | Default | Description |
|---|---|---|
| `incremental_candidates_lookback_days` | 60 | Lookback window for build history analysis |
| `incremental_candidates_min_build_time_sec` | 300 | Minimum max build time to trigger on build time alone |
| `incremental_candidates_min_size_gb` | 2 | Minimum table size to trigger on size alone |
| `incremental_candidates_min_compute_waste_score` | 5 | Minimum size x builds/day score to trigger on waste alone |
| `incremental_candidates_min_compute_waste_avg_build_sec` | 30 | Minimum average build time alongside waste score trigger |
| `incremental_candidates_min_qualified_build_days` | 3 | Minimum CTAS build days to trust growth signal |
