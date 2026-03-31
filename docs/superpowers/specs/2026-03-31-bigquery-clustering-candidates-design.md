# BigQuery Clustering Candidates — Design Spec

**Date:** 2026-03-31
**Status:** Approved

## Context

The package already has a complete Snowflake clustering candidates pipeline:
`stg_snowflake__*` → `int_snowflake__*` → `int_table_inventory` / `int_table_query_stats_daily` → `fct_snowflake__table_clustering_candidates`

The BigQuery staging models exist (`stg_bigquery__*`) and two intermediate models exist (`int_bigquery__tables`, `int_bigquery__table_storage`) but they are incomplete. The two platform-agnostic router models (`int_table_inventory`, `int_table_query_stats_daily`) already reference the missing BigQuery intermediate models — they just don't exist yet.

## Goal

1. **Option B** — Build a complete, BQ-native pipeline: fill in the missing BQ intermediate models and create `fct_bigquery__table_clustering_candidates` with a scoring formula suited to BigQuery's cost model (bytes billed, slot time, partition count).
2. **Option C** — Build a single `fct__table_clustering_candidates` that compiles and runs on all platforms, routing through the existing platform-agnostic intermediate models. Jinja handles SQL syntax differences and the BQ-specific scoring branch.

## Files Changed

### Updated (no Snowflake model changes)

| File | Change |
|---|---|
| `models/intermediate/bigquery/int_bigquery__tables.sql` | Join `stg_bigquery__table_storage` to populate `row_count`, `is_deleted`; parse DDL for `clustering_key` |
| `models/intermediate/bigquery/int_bigquery__table_storage.sql` | Pass through `total_rows` and `total_partitions` from staging for use by inventory model |

### New intermediate models

| File | Purpose |
|---|---|
| `models/intermediate/bigquery/int_bigquery__table_inventory.sql` | Joins `int_bigquery__tables` + `int_bigquery__table_storage`; outputs same schema as `int_snowflake__table_inventory` |
| `models/intermediate/bigquery/int_bigquery__table_query_stats_daily.sql` | Daily query stats per table via query-text matching; outputs same schema as Snowflake counterpart with BQ-specific column mappings |

### New fact models

| File | Purpose |
|---|---|
| `models/marts/bigquery/fct_bigquery__table_clustering_candidates.sql` | Option B: BQ-native scoring using `select_bytes_billed_sum` and `total_slot_ms` |
| `models/marts/fct__table_clustering_candidates.sql` | Option C: unified cross-platform model; Jinja for syntax and scoring branch |

### Documentation

| File | Change |
|---|---|
| `models/intermediate/_intermediate.yml` | Add column docs for the 2 new BQ intermediate models |

## Column Naming Convention

Column names in the BQ intermediate models match Snowflake counterparts exactly. Names are Snowflake-coined — a future refactor could make them platform-neutral.

### `int_{platform}__table_inventory` contract

| Column | Snowflake value | BigQuery value |
|---|---|---|
| `approx_micropartitions` | `active_bytes / (16 * 1024 * 1024)` — micropartition approximation | `total_partitions` — actual BQ partition count from `TABLE_STORAGE` |

### `int_{platform}__table_query_stats_daily` contract

| Column | Snowflake value | BigQuery value |
|---|---|---|
| `select_execution_time_ms_sum` | sum of `execution_time_ms` (wall-clock ms) | sum of `total_slot_ms` (parallel CPU time) |
| `select_partitions_scanned_sum` | sum of `partitions_scanned` | `0` — not available at query level in BQ |
| `select_partitions_total_sum` | sum of `partitions_total` | `0` — score formula falls back to `approx_micropartitions` |
| `select_bytes_billed_sum` *(BQ extra)* | `0` | sum of `total_bytes_billed` — primary BQ cost signal |

`select_bytes_billed_sum` is a BQ-only extra column. It flows through the `int_table_query_stats_daily` router (`select *`) and is used by both `fct_bigquery__table_clustering_candidates` (Option B) and the BQ scoring branch in `fct__table_clustering_candidates` (Option C).

## Model Designs

### `int_bigquery__tables` (updated)

Joins `stg_bigquery__tables` (for table type, DDL) with `stg_bigquery__table_storage` (for `total_rows`, `deleted`) on `table_catalog = project_id` + `table_schema` + `table_name`. Extracts `clustering_key` via `regexp_extract(ddl, r'(?i)CLUSTER BY (.+?)(?:\n|;|$)')`.

### `int_bigquery__table_storage` (updated)

Adds `total_rows` and `total_partitions` passthrough from `stg_bigquery__table_storage`. These are BQ-specific extra columns used by `int_bigquery__table_inventory`; they don't break the unified `int_table_storage` schema.

### `int_bigquery__table_inventory` (new)

Mirrors `int_snowflake__table_inventory`. Joins `int_bigquery__tables` + `int_bigquery__table_storage`. Key mappings:
- `size_gb` = `active_bytes / pow(1024, 3)`
- `is_already_clustered` = `clustering_key is not null`
- `approx_micropartitions` = `total_partitions` (with comment)
- `normalized_table_type` derived from `table_type`
- Filters to `table_type in ('BASE TABLE', 'MATERIALIZED VIEW')`, excludes deleted

### `int_bigquery__table_query_stats_daily` (new)

Mirrors `int_snowflake__table_query_stats_daily` structure. Query-to-table attribution via `query_text ilike '%' || table_name || '%'` (same fallback as Snowflake Standard edition). DML statement types: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE_TABLE_AS_SELECT`. Column mappings documented above.

### `fct_bigquery__table_clustering_candidates` (Option B)

References `int_bigquery__table_inventory` and `int_bigquery__table_query_stats_daily` directly (not through routers) for access to BQ-specific columns. BQ-native score formula:

```
score = (
    select_count * avg_gb_billed_per_query        -- volume × cost per query
    + (query_to_dml_ratio * 10)                   -- read-heavy bonus
)
* partition_density_multiplier                    -- total_partitions / row_count proxy
```

`is_candidate` logic is the same as Snowflake: `select_count > 0 AND query_to_dml_ratio > 1 AND size_gb >= min_size_gb`.

BQ-specific SQL: `if()` instead of `iff()`, `date_sub()` instead of `dateadd()`, `to_hex(md5())` for surrogate key, `cast(... as string)` instead of `to_varchar()`.

### `fct__table_clustering_candidates` (Option C)

References `int_table_inventory` and `int_table_query_stats_daily` (the platform-agnostic routers). Uses `{{ dbt.dateadd() }}` for cross-platform date arithmetic. Uses `CASE WHEN` instead of `iff()`. Jinja for `md5` encoding and score formula:

```jinja
{% if target.type == 'bigquery' %}
  -- BQ: score on bytes billed (primary cost signal) + slot time
{% else %}
  -- Snowflake/others: score on execution time + partition density
{% endif %}
```

The BQ branch accesses `select_bytes_billed_sum` (flows through the router's `select *`). The non-BQ branch uses `select_execution_time_ms_sum` and partition columns.

## Constraints

- No changes to any Snowflake models
- `int_bigquery__table_query_stats_daily` uses query-text matching for attribution (no `referenced_tables` in current `stg_bigquery__jobs_by_project`)
- `clustering_candidates_min_size_gb` default of 1000 GB is Snowflake-tuned; BQ users will likely want to override it lower (BQ benefits from clustering at much smaller sizes), but that's a vars concern not a model concern
- Both fact models are `incremental` with `merge` strategy, same as Snowflake
