# Incremental Model Candidates

This document covers the pipeline for identifying dbt models that are materialized as `table` but scan enough data per run to justify converting to `incremental` materialization.

## Pipeline Overview

```
Staging                              Intermediate                               Mart
───────                              ────────────                               ────
stg_databricks__query_history    ──► int_databricks__dbt_model_run_history  ──►
  (system.query.history)                                                         fct_databricks__
stg_databricks__columns          ──► int_databricks__column_cluster_suggestions  incremental_model_
  (information_schema.columns)   ──►                                             candidates
                                  int_dbt__relations
                                  int_table_inventory
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
| `int_databricks__column_cluster_suggestions` | Scores each table's columns by query filter frequency to suggest the best cluster/filter column. Used here to populate `suggested_filter_column`. |

### Fact model

**`fct_databricks__incremental_model_candidates`** aggregates model run history over the lookback window, flags `table`-materialized models that scan above the threshold as candidates for conversion to `incremental`, and surfaces copy-pasteable starter code so engineers can act on the recommendation directly.

**Key CTEs in the fact model:**

| CTE | Purpose |
|-----|---------|
| `model_runs` | Aggregates run history from `int_databricks__dbt_model_run_history` over the lookback window. |
| `table_dml_stats` | Sums INSERT/UPDATE/DELETE/MERGE counts from `int_databricks__table_query_stats_daily` to determine update semantics. |
| `table_clustered` | Looks up whether each table already has a cluster key from `int_table_inventory`. |
| `unique_key_candidates` / `suggested_unique_keys` | Reads `stg_databricks__columns` and scores column names by pattern (`_id`, `id`, `uuid`, `guid`, `_key`) to suggest a merge unique key. The top-scoring column per table is surfaced as `suggested_unique_key`. |
| `filter_column_suggestions` | Pulls the top suggested filter/cluster column per table from `int_databricks__column_cluster_suggestions`. |
| `filter_column_data_types` | Joins `stg_databricks__columns` with the filter column suggestion to retrieve that column's `data_type` — needed to detect microbatch candidates. |
| `final` | Joins all of the above, computes scores, flags candidates, and derives `suggested_incremental_strategy`. |
| `final_with_templates` | Wraps `final` to add four template string columns (`recommendation_reason`, `incremental_filter_template`, `updated_model_config`, `microbatch_config_template`) that produce copy-pasteable dbt code. |

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
| `insert_count` | Number of INSERT statements observed against this table in the lookback window |
| `update_count` | Number of UPDATE statements observed against this table in the lookback window |
| `delete_count` | Number of DELETE statements observed against this table in the lookback window |
| `merge_count` | Number of MERGE statements observed against this table in the lookback window |
| `suggested_incremental_strategy` | Recommended dbt incremental strategy based on DML history: `merge`, `append`, or `insert_overwrite` |
| `suggested_incremental_strategy_confidence` | `HIGH`, `MEDIUM`, or `LOW` — how confident the suggestion is (see below) |
| `first_seen` | Earliest run timestamp in the lookback window |
| `last_seen` | Most recent run timestamp in the lookback window |
| `suggested_unique_key` | Best candidate column name for use as the `unique_key` in a `merge` strategy. Scored by naming pattern: `_id` suffix (score 9) > `id` exact (8) > `uuid`/`guid` exact (7) > `_uuid`/`_guid`/`_key` suffix (6). `null` if no qualifying column is found. |
| `suggested_filter_column` | Best candidate column for the `{% if is_incremental() %}` filter predicate, sourced from `int_databricks__column_cluster_suggestions`. `null` if no suggestion is available. |
| `suggested_filter_column_confidence` | Confidence level for `suggested_filter_column`, inherited from `int_databricks__column_cluster_suggestions`. |
| `filter_column_data_type` | Data type of `suggested_filter_column`. Used internally to detect microbatch eligibility; also useful for validating filter syntax. |
| `recommendation_reason` | Human-readable summary of why this model is a candidate (e.g. "Avg 1.5 GB scanned per run across 14 runs in the last 7 days — incrementalization would reduce compute cost"). `null` for non-candidates. |
| `incremental_filter_template` | Copy-pasteable SQL snippet for the `{% if is_incremental() %}` filter block, pre-filled with `suggested_filter_column` and the appropriate predicate for the suggested strategy. |
| `updated_model_config` | Copy-pasteable `{{ config(...) }}` block with `materialized='incremental'`, `incremental_strategy`, `unique_key` (if applicable), and `on_schema_change='append_new_columns'` pre-filled. |
| `microbatch_config_template` | Copy-pasteable microbatch `{{ config(...) }}` block. Only populated when: strategy is `merge`, filter column is a timestamp type, and the table is insert-heavy (insert_count > 9× update+merge, or zero updates). `null` otherwise. |

---

## Understanding `is_candidate`

A model is flagged as `is_candidate = true` when **all three** conditions are met:

1. **It is a `table` materialization.** `incremental` models are already processing only new data. `view` models don't scan data at dbt run time — they execute at query time — so they are not flagged here.

2. **It scans enough data per run.** `avg_bytes_scanned_gb >= incremental_candidates_min_avg_bytes_scanned_gb` — models below the threshold are too small to justify the added complexity of incremental logic.

3. **It runs frequently enough to establish a pattern.** `run_count >= incremental_candidates_min_run_count` — a model that ran once in the lookback window may be a one-off or a rare full refresh, not a regular scheduled run.

---

## Understanding `suggested_incremental_strategy`

### Why this is harder than it sounds

Choosing the right incremental strategy requires knowing the *update semantics* of your data — whether rows are ever modified after insertion, whether records can be deleted, and whether there is a reliable unique key. These are application-level facts that no data platform exposes directly in system tables.

To approximate this, the model uses **DML type breakdown** from `system.query.history`. Databricks records the `statement_type` of every query (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`), which gives us a signal about how data actually flows into each table. This is the same approach taken in the Snowflake version of this package.

**Important attribution note:** DML counts use the same fuzzy text-matching approach as the table query stats pipeline (`query_text ilike '%table_name%'`). This means DML from *any* source is captured — dbt runs, Spark jobs, manual SQL, external pipelines. That breadth is intentional: if something outside dbt is updating this table, that's important signal for choosing a strategy. But it also means the counts can include false positives if a table name is a common substring.

### Decision matrix

| Condition | Suggested strategy | Confidence |
|---|---|---|
| `update_count > 0` or `merge_count > 0` | `merge` | `HIGH` — rows are definitively mutable; append would lose updates |
| `delete_count > 0`, no updates or merges | `merge` | `MEDIUM` — deletions require handling; merge covers this safely |
| Only inserts, table has a partition/cluster column | `insert_overwrite` | `LOW` — pattern suggests partition-aligned writes, but not confirmed |
| Only inserts, no partition/cluster column | `append` | `LOW` — likely immutable, but cannot be confirmed from system tables alone |
| No DML history in lookback window | `merge` | `LOW` — safest default when no signal exists |

### Confidence levels

| Value | Meaning |
|-------|---------|
| `HIGH` | UPDATE or MERGE statements observed — rows are definitely mutable. `merge` is the correct strategy. |
| `MEDIUM` | DELETE statements observed with no updates — rows may be deleted but not modified. `merge` is still the safe choice. |
| `LOW` | Only inserts observed, or no DML history at all. The suggestion is a reasonable starting point but requires validation. |

### What this cannot tell you

- **Whether `suggested_unique_key` is actually unique.** The model scores columns by name pattern (`_id`, `id`, `uuid`, etc.) but cannot verify uniqueness from system tables. Always run `count(*) vs count(distinct <col>)` before using the suggestion.
- **Whether `append` is truly safe.** Even if no UPDATE statements are observed in the lookback window, out-of-band corrections or delayed reprocessing jobs could invalidate the append assumption. Always confirm with the team that owns the source data.
- **The right predicate for `insert_overwrite`.** If you use `insert_overwrite`, dbt needs to know which partition(s) to replace. Review the `suggested_cluster_key` in `fct_databricks__liquid_clustering_candidates` for guidance on which column is most commonly filtered.
- **Whether the microbatch template is the right final config.** `microbatch_config_template` is only surfaced when conditions strongly suggest it (timestamp filter column + insert-heavy pattern), but you should verify the `event_time` column truly represents event time and set a realistic `begin` date before using it.

### Applying the suggestion

The fact model surfaces four template columns that produce copy-pasteable dbt starter code. Retrieve them for a specific model and paste the output directly into your `.sql` file:

```sql
-- Get copy-pasteable starter code for a specific model
select
    model_name,
    table_fqn,
    recommendation_reason,
    suggested_incremental_strategy,
    suggested_incremental_strategy_confidence,
    suggested_unique_key,
    suggested_filter_column,
    incremental_filter_template,
    updated_model_config,
    microbatch_config_template
from <your_catalog>.<your_schema>.fct_databricks__incremental_model_candidates
where snapshot_date = current_date()
    and is_candidate = true
order by score desc;
```

`updated_model_config` contains a ready-to-use `{{ config(...) }}` block, and `incremental_filter_template` contains the corresponding `{% if is_incremental() %}` WHERE clause. For example, a merge candidate with `suggested_unique_key = 'event_id'` and `suggested_filter_column = 'created_at'` would produce:

```sql
-- updated_model_config:
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

-- incremental_filter_template (add this to the end of your model's WHERE clause):
{% if is_incremental() %}
    where created_at >= (
        select max(created_at) - INTERVAL 1 DAY
        from {{ this }}
    )
{% endif %}
```

If `microbatch_config_template` is non-null, it means the table pattern (insert-heavy + timestamp filter column) is consistent with microbatch. Review the template and set the `begin` date before using it.

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
    is_candidate,
    recommendation_reason,
    suggested_incremental_strategy,
    suggested_unique_key,
    suggested_filter_column
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
