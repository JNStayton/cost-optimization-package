# Incremental Model Candidates

This document covers the pipeline for identifying dbt models that are materialized as `table` but scan enough data per run to justify converting to `incremental` materialization.

## Pipeline Overview

```
Staging                              Intermediate                               Mart
───────                              ────────────                               ────
stg_databricks__query_history    ──► int_databricks__dbt_model_run_history  ──► fct_databricks__
  (system.query.history)                                                          incremental_model_
                                  int_dbt__relations                              candidates
```

### How query-to-model attribution works

Unlike the table query stats pipeline (which uses fuzzy table name matching), this model uses **exact attribution** via the dbt query comment. Every query dbt runs is prefixed with a JSON comment:

```sql
/* {"app": "dbt", "dbt_version": "1.x.x", "node_id": "model.my_project.my_model", ...} */
SELECT ...
```

The intermediate model extracts `node_id` from this comment using `regexp_extract` and joins it directly to `int_dbt__relations` on the `dbt_model` (unique_id) field. This means attribution is exact — only queries from the current dbt project are captured, and each query is attributed to the specific model that ran it.

### Staging layer

| Model | Source | Materialization |
|-------|--------|-----------------|
| `stg_databricks__query_history` | `system.query.history` | incremental (7-day lookback) |

### Intermediate layer

| Model | Purpose |
|-------|---------|
| `int_databricks__dbt_model_run_history` | Filters query history to dbt-executed queries, extracts `node_id` from the comment header, and joins to `int_dbt__relations` to get model name and materialization type. One row per successful dbt model run. |
| `int_dbt__relations` | Compile-time mapping from dbt model metadata to physical relation names and materialization types. |

### Fact model

**`fct_databricks__incremental_model_candidates`** aggregates model run history over the lookback window and flags `table`-materialized models that scan above the threshold as candidates for conversion to `incremental`.

---

## Why convert `table` models to `incremental`?

A `table` materialized dbt model performs a full refresh on every run — it reads all source data, transforms it, and rewrites the entire output table. For models that process large historical datasets but only receive new rows over time, this is wasteful: 99% of the data is unchanged, but 100% of it is scanned and rewritten each run.

An `incremental` model instead processes only rows that are new or changed since the last run. For a model that runs daily on a 2-year dataset, converting from `table` to `incremental` can reduce bytes scanned by 99%+ and cut runtime from minutes to seconds.

**Good candidates for incremental conversion:**
- Event tables, log tables, or any append-only source
- Models with a reliable timestamp column that identifies new rows
- Models that run frequently (daily or more often)
- Models that scan significantly more data than they produce as new rows

**Poor candidates:**
- Models that perform complex historical recalculations (SCD Type 2, rolling windows)
- Models where source data is regularly updated or deleted
- Small models where the overhead of incremental logic outweighs the benefit

---

## Project Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `incremental_candidates_lookback_days` | `7` | Number of days of run history to analyze. |
| `incremental_candidates_min_avg_bytes_scanned_gb` | `0.1` | Minimum average bytes scanned per run (in GB) to flag as a candidate. Models below this threshold are too small to benefit meaningfully from incremental conversion. |
| `incremental_candidates_min_run_count` | `3` | Minimum number of runs within the lookback window required to flag as a candidate. Requires enough runs to establish a reliable pattern. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  # Widen the analysis window
  incremental_candidates_lookback_days: 14

  # Lower threshold for smaller projects
  incremental_candidates_min_avg_bytes_scanned_gb: 0.01
```

---

## Output Columns

| Column | Description |
|--------|-------------|
| `snapshot_date` | Date of the snapshot (one per day) |
| `dbt_model` | dbt `unique_id` (e.g. `model.my_project.my_model`) |
| `model_name` | Short model name |
| `materialized` | Current materialization: `table`, `view`, or `incremental` |
| `table_fqn` | Fully qualified table name (`catalog.schema.table`) |
| `database_name` | Catalog |
| `schema_name` | Schema |
| `table_name` | Table name |
| `run_count` | Number of successful runs in the lookback window |
| `total_bytes_scanned_gb` | Total bytes scanned across all runs in the lookback window |
| `avg_bytes_scanned_gb` | Average bytes scanned per run |
| `total_execution_time_s` | Total execution time across all runs in seconds |
| `avg_execution_time_s` | Average execution time per run in seconds |
| `estimated_monthly_runs` | Extrapolated monthly run count based on the lookback window |
| `estimated_monthly_bytes_scanned_gb` | Extrapolated monthly bytes scanned if the model stays as `table` |
| `score` | `avg_bytes_scanned_gb × run_count` — higher means more total data could be saved by converting |
| `is_candidate` | `true` when materialized = `table`, avg bytes ≥ threshold, and run count ≥ minimum |
| `first_seen` | Earliest run timestamp in the lookback window |
| `last_seen` | Most recent run timestamp in the lookback window |

---

## Understanding `is_candidate`

A model is flagged as `is_candidate = true` when **all three** conditions are met:

1. **It is a `table` materialization.** `incremental` models are already processing only new data. `view` models don't scan data at dbt run time — they execute at query time — so they are not flagged here.

2. **It scans enough data per run.** `avg_bytes_scanned_gb >= incremental_candidates_min_avg_bytes_scanned_gb` — models below the threshold are too small to justify the added complexity of incremental logic.

3. **It runs frequently enough to establish a pattern.** `run_count >= incremental_candidates_min_run_count` — a model that ran once in the lookback window may be a one-off or a rare full refresh, not a regular scheduled run.

---

## Sample Queries

### Top candidates by estimated monthly savings

```sql
select
    model_name,
    table_fqn,
    run_count,
    avg_bytes_scanned_gb,
    avg_execution_time_s,
    estimated_monthly_runs,
    estimated_monthly_bytes_scanned_gb,
    score,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__incremental_model_candidates
where snapshot_date = current_date()
order by estimated_monthly_bytes_scanned_gb desc;
```

### All table models regardless of candidate status

```sql
select
    model_name,
    materialized,
    run_count,
    avg_bytes_scanned_gb,
    avg_execution_time_s,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__incremental_model_candidates
where snapshot_date = current_date()
    and materialized = 'table'
order by avg_bytes_scanned_gb desc;
```

---

## Notes

- **Only current project models are captured.** Attribution relies on the `node_id` in the dbt query comment, which is scoped to the project running the pipeline. Queries from other dbt projects or ad-hoc SQL are excluded.
- **`statement_text` must be populated.** If your workspace uses customer-managed keys, query text is redacted and this pipeline will return no rows. Check with your workspace admin.
- **Views are excluded.** dbt creates views with a fast DDL statement that scans near-zero bytes. View query costs are incurred at query time, not at dbt run time, and are outside the scope of this model.
- **Incremental conversion requires a reliable incremental key.** Before converting a model, confirm the source has a timestamp or ID column that reliably identifies new rows. Without a good incremental key, the model may miss updates or require frequent full refreshes anyway.
