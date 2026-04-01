# Table Clustering Candidates

This document covers the end-to-end pipeline for identifying Snowflake tables that may benefit from clustering, from staging through the fact model, with emphasis on configuration and how to interpret the results.

## Pipeline Overview

```
Staging                          Intermediate                          Mart
───────                          ────────────                          ────
stg_snowflake__tables        ──► int_snowflake__tables        ─┐
                                                                ├──► int_snowflake__table_inventory ─┐
stg_snowflake__table_        ──► int_snowflake__table_         │                                     │
  storage_metrics                  storage                    ─┘                                     │
                                                                                                     ├──► fct_snowflake__table_
stg_snowflake__query_        ──► int_snowflake__query_                                               │      clustering_candidates
  history                          history                    ─┐                                     │
                                                                ├──► int_snowflake__table_query_  ───┘
stg_snowflake__access_       ──► int_snowflake__query_         │      stats_daily
  history (Enterprise)             table_access               ─┘

                                 int_dbt__relations           ─────────────────────────────────────────┘
```

### Staging layer

| Model | Source | Materialization |
|-------|--------|-----------------|
| `stg_snowflake__tables` | `SNOWFLAKE.ACCOUNT_USAGE.TABLES` | view |
| `stg_snowflake__table_storage_metrics` | `SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS` | view |
| `stg_snowflake__query_history` | `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | incremental (7-day lookback) |
| `stg_snowflake__access_history` | `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` | incremental (7-day lookback, Enterprise only) |

### Intermediate layer

| Model | Purpose |
|-------|---------|
| `int_snowflake__table_inventory` | Joins table metadata with storage metrics. Produces one row per active table with size, row count, micropartition estimate, and clustering state. |
| `int_snowflake__table_query_stats_daily` | Daily aggregated query statistics per table. Attribution uses ACCESS_HISTORY (Enterprise) or `query_text` matching (Standard). |
| `int_snowflake__query_table_access` | Flattens ACCESS_HISTORY into one row per (query, table) for exact object-level attribution. Enterprise only. |
| `int_dbt__relations` | Compile-time mapping from dbt model metadata to physical relation names. Used to attribute results back to dbt models. |

Platform-agnostic routers (`int_table_inventory`, `int_table_query_stats_daily`) sit between the platform-specific intermediate models and the fact. The fact references these routers, not the Snowflake-specific models directly.

### Fact model

**`fct_snowflake__table_clustering_candidates`** is the final output. It is an incremental model (merge strategy, one snapshot per day) that scores tables and flags clustering candidates based on query activity, table size, and partition efficiency.

---

## Project Variables

Set these in your `dbt_project.yml` under `vars:` to customize behavior.

### Attribution and edition

| Variable | Default | Description |
|----------|---------|-------------|
| `use_access_history_attribution` | `true` | Set to `false` for Snowflake Standard edition (no ACCESS_HISTORY view). When `false`, query-to-table attribution falls back to `query_text` matching. |

### Query stats scope and performance

| Variable | Default | Description |
|----------|---------|-------------|
| `table_query_stats_full_account` | `false` | When `false` (default), only collects query stats for tables that are dbt models in the current project. Set to `true` to scan all tables in the Snowflake account. |
| `table_query_stats_initial_lookback_days` | `30` (Enterprise) / `7` (Standard) | Number of days of query history to process on the first build. Subsequent incremental runs pick up from the last processed date. Override to widen or narrow the initial window. |

### Fact model tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `clustering_candidates_min_size_gb` | `100` | Minimum table size in GB to evaluate. Only tables at or above this threshold appear in results. Set to a lower value (e.g. `1`) for dev/sandbox environments. |
| `clustering_candidates_lookback_days` | `7` | Number of days of daily query stats to aggregate when computing scores and metrics. |
| `clustering_candidates_dbt_project_only` | `true` | When `true`, only tables that match a dbt model in the current project are included in the output. Set to `false` to include all tables that meet the size threshold. |
| `clustering_candidates_target_databases` | `[]` | Optional list of database names to restrict evaluation to. Empty = no restriction. |
| `clustering_candidates_target_schemas` | `[]` | Optional list of schema names to restrict evaluation to. Empty = no restriction. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  # Standard edition Snowflake
  use_access_history_attribution: false

  # Include smaller tables for testing
  clustering_candidates_min_size_gb: 1

  # Widen the analysis window
  clustering_candidates_lookback_days: 14
```

---

## Output Columns

| Column | Description |
|--------|-------------|
| `analyzed_at` | Timestamp when the snapshot was computed |
| `snapshot_date` | Date of the snapshot (one per day) |
| `database_name` | Database containing the table |
| `schema_name` | Schema containing the table |
| `table_name` | Table name |
| `table_fqn` | Fully qualified name (`DATABASE.SCHEMA.TABLE`) |
| `dbt_model` | dbt `unique_id` if the table is a dbt model, otherwise `null` |
| `table_type` | Human-readable type: Permanent Table, Transient Table, or Materialized View |
| `score` | Composite score combining query volume, execution time, read/write ratio, and partition skew. Higher = more potential benefit from clustering. |
| `is_candidate` | Whether the table meets all three criteria for clustering (see below) |
| `table_size_gb` | Table size in GB |
| `total_rows` | Approximate row count |
| `current_micropartitions` | Current micropartition count (from query stats or estimated from storage) |
| `avg_rows_per_micropartition` | Average rows per micropartition |
| `avg_partitions_scanned` | Average micropartitions scanned per SELECT query |
| `select_count` | Number of SELECT queries against the table in the lookback window |
| `dml_count` | Number of INSERT/UPDATE/DELETE/MERGE operations in the lookback window |
| `query_to_dml_ratio` | `select_count / (dml_count + 1)` — higher means more read-heavy |
| `avg_query_duration_s` | Average SELECT execution time in seconds |

---

## Understanding `is_candidate`

A table is flagged as `is_candidate = true` only when **all three** conditions are met:

1. **The table is actively read.** `select_count > 0` — at least one SELECT query was executed against the table during the lookback window.

2. **Reads outnumber writes.** `query_to_dml_ratio > 1` — the table receives more SELECT queries than DML operations (INSERT, UPDATE, DELETE, MERGE). Clustering reorganizes data on disk to improve read performance, but every write operation can disrupt that organization. Tables with heavy write activity will see clustering benefits eroded quickly, increasing automatic reclustering costs.

3. **The table meets the minimum size threshold.** `table_size_gb >= clustering_candidates_min_size_gb` — clustering overhead (compute cost for automatic reclustering, metadata management) is typically not justified for small tables. The default threshold is 100 GB.

### When `is_candidate` is `false` but the data is still useful

A table may have a **high score** but `is_candidate = false`. This happens when the table has significant query activity and partition skew but fails the read/write ratio check. The score and is_candidate measure different things:

- **Score** = "How much query activity and partition inefficiency exists?" — a raw signal of potential benefit.
- **is_candidate** = "Is clustering practically viable given the read/write mix and table size?"

Tables with high scores but `is_candidate = false` are still worth investigating. For example:

- A table with a 0.5 query-to-DML ratio but very high `avg_partitions_scanned` may still benefit from clustering if the writes are batch loads (e.g. nightly) and the reads happen throughout the day. In that pattern, automatic reclustering has time to reorganize between write batches.
- A table just below the size threshold may still benefit if its queries are slow due to poor partition pruning.

Use the full set of columns (not just `is_candidate`) to make informed decisions about which tables to test clustering on.

---

## Sample Queries

### Top clustering candidates by score

```sql
select
    table_fqn,
    dbt_model,
    score,
    is_candidate,
    table_size_gb,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_partitions_scanned,
    current_micropartitions,
    avg_query_duration_s
from <your_database>.<your_schema>.fct_snowflake__table_clustering_candidates
where snapshot_date = current_date()
order by score desc;
```

### Candidate tables with high partition scan ratios

Tables where queries scan a large proportion of available micropartitions are the ones most likely to see immediate improvement from clustering.

```sql
select
    table_fqn,
    dbt_model,
    score,
    current_micropartitions,
    avg_partitions_scanned,
    round(avg_partitions_scanned / nullif(current_micropartitions, 0) * 100, 1) as pct_partitions_scanned,
    select_count,
    avg_query_duration_s
from <your_database>.<your_schema>.fct_snowflake__table_clustering_candidates
where snapshot_date = current_date()
    and select_count > 0
order by pct_partitions_scanned desc;
```

### Historical trend for a specific table

```sql
select
    snapshot_date,
    score,
    is_candidate,
    select_count,
    dml_count,
    avg_partitions_scanned,
    avg_query_duration_s
from <your_database>.<your_schema>.fct_snowflake__table_clustering_candidates
where table_fqn = 'MY_DATABASE.MY_SCHEMA.MY_TABLE'
order by snapshot_date;
```

---

## Notes

- **Snowflake edition:** Enterprise edition (or higher) is recommended. Enterprise uses ACCESS_HISTORY for exact query-to-table attribution. Standard edition falls back to `query_text` matching, which is less precise and slower on high-volume accounts.
- **Account usage latency:** Snowflake's ACCOUNT_USAGE views can lag by 45 minutes to 3 hours. Run the pipeline after this latency window for complete results.
- **First build performance:** The first run processes the full initial lookback window (default 30 days for Enterprise, 7 days for Standard). Subsequent incremental runs only process new data and are significantly faster.
- **Incremental snapshots:** The fact model produces one snapshot per day. Running it multiple times in the same day updates (merges) the existing snapshot for that date rather than creating duplicates.
