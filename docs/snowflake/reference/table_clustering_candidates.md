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
| `stg_snowflake__table_query_pruning_history` | `SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY` | incremental (7-day lookback) |

### Intermediate layer

| Model | Purpose |
|-------|---------|
| `int_snowflake__table_inventory` | Joins table metadata with storage metrics. Produces one row per active table with size, row count, micropartition estimate, and clustering state. |
| `int_snowflake__table_query_stats_daily` | Daily aggregated query statistics per table. Attribution uses ACCESS_HISTORY (Enterprise) or `query_text` matching (Standard). |
| `int_snowflake__query_table_access` | Flattens ACCESS_HISTORY into one row per (query, table) for exact object-level attribution. Enterprise only. |
| `int_snowflake__query_operator_columns` | Stores parsed Filter/Join column references from GET_QUERY_OPERATOR_STATS. Populated by the `extract_query_operator_columns` macro. |
| `int_dbt__relations` | Compile-time mapping from dbt model metadata to physical relation names. Used to attribute results back to dbt models and find downstream children for operator analysis. |

The fact references the Snowflake-specific intermediate models directly.

### Fact model

**`fct_snowflake__table_clustering_candidates`** is the final output. It is an incremental model (merge strategy, one snapshot per day) that scores tables and flags clustering candidates based on query activity, table size, and partition efficiency.

**Post-hooks (execute in order after the fact model builds):**

1. **`refresh_column_cardinality()`** — profiles APPROX_COUNT_DISTINCT for columns on the top N candidate tables
2. **`extract_query_operator_columns()`** — analyzes query plan operators (GET_QUERY_OPERATOR_STATS) to identify which columns are used in Filter/Join conditions

**`fct_snowflake__clustering_key_candidates`** runs after the post-hooks and uses both cardinality and operator data to score individual columns and recommend clustering keys.

### Clustering key pipeline

```
fct_snowflake__table_clustering_candidates (identifies WHICH tables)
    │
    ├─ post-hook: refresh_column_cardinality()
    │   └─ writes to: int_snowflake__column_cardinality
    │
    ├─ post-hook: extract_query_operator_columns()
    │   ├─ reads: int_snowflake__column_query_access (candidate + child FQNs)
    │   ├─ reads: int_snowflake__query_history (parameterized hash dedup)
    │   ├─ reads: int_dbt__relations (downstream children lookup)
    │   ├─ calls: GET_QUERY_OPERATOR_STATS per query_id
    │   └─ writes to: int_snowflake__query_operator_columns
    │
    └─► fct_snowflake__clustering_key_candidates (scores WHICH columns)
        ├─ reads: int_snowflake__query_operator_columns (filter/join evidence)
        ├─ reads: int_snowflake__column_cardinality (cardinality profiles)
        └─ applies: 25% proportion gate (column must appear in >= 25% of analyzed queries)
```

**Downstream children expansion:** The extract macro doesn't only look at queries that directly accessed the candidate table — it also finds queries against the candidate's direct child models (from `int_dbt__relations.parent_models`). This captures filter evidence from analyst queries hitting downstream marts that read from the candidate.

**Proportion gating:** A column is only recommended as a clustering key if it appears in Filter operators in >= 33% of the analyzed queries. Join-only columns are excluded entirely — filter usage is the admission ticket, join usage is the scoring bonus.

**Diminishing returns gate:** The 2nd and 3rd recommended keys must score at least 50% of the top key's score for that table. This prevents low-impact columns from riding alongside a strong primary key and ensures each additional key in the clustering spec is worth the maintenance overhead.

**Scoring formula:** `filter_query_count * 3 + join_query_count * 1 + cardinality_bonus` where the cardinality bonus rewards columns with 100-10000 rows per distinct value (sweet spot for micropartition grouping). Filter usage is weighted 3x because WHERE predicates directly enable partition pruning. Join usage is weighted 1x as a secondary signal (co-location benefit for hash joins is marginal).

---

## Project Variables

Set these in your `dbt_project.yml` under `vars:` to customize behavior.

### Attribution and edition

| Variable | Default | Description |
|----------|---------|-------------|
| `snowflake_enterprise_edition` | `true` | Set to `false` for Snowflake Standard edition (no ACCESS_HISTORY view). When `false`, query-to-table attribution falls back to `query_text` matching. |

### Query stats scope and performance

| Variable | Default | Description |
|----------|---------|-------------|
| `table_query_stats_full_account` | `false` | When `false` (default), only collects query stats for tables that are dbt models in the current project. Set to `true` to scan all tables in the Snowflake account. |
| `incremental_overlap_days` | `31` | Number of days of data to process on every build (including first). Set to your longest gap between package builds. |

### Fact model tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `clustering_candidates_min_size_gb` | `100` | Minimum table size in GB to evaluate. Only tables at or above this threshold appear in results. Set to a lower value (e.g. `1`) for dev/sandbox environments. |
| `clustering_candidates_lookback_days` | `7` | Number of days of daily query stats to aggregate when computing scores and metrics. |
| `clustering_candidates_dbt_project_only` | `true` | When `true`, only tables that match a dbt model in the current project are included in the output. Set to `false` to include all tables that meet the size threshold. |
| `clustering_candidates_target_databases` | `[]` | Optional list of database names to restrict evaluation to. Empty = no restriction. |
| `clustering_candidates_target_schemas` | `[]` | Optional list of schema names to restrict evaluation to. Empty = no restriction. |
| `clustering_key_cardinality_table_limit` | `10` | Maximum number of top candidate tables (by score) to evaluate for column-level clustering key recommendations in `fct_snowflake__clustering_key_candidates`. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  # Standard edition Snowflake
  snowflake_enterprise_edition: false

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
| `score` | V3 composite score. Higher = more potential benefit from clustering. See [Scoring Logic](#scoring-logic) below. |
| `is_candidate` | Whether the table meets all four criteria for clustering (see [Understanding `is_candidate`](#understanding-is_candidate-and-recommendation_tier) below) |
| `recommendation_tier` | Impact-based classification: High impact, Moderate impact, Low impact, Healthy, Write-heavy, No read activity. See tiers section below. |
| `recommendation_reason` | Human-readable explanation of why the table was or was not flagged. States the partition scan %, read count, duration, and ratio for candidates; states which criterion was not met for non-candidates. |
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

## Scoring Logic

### V3 formula (current)

```
score = total_read_time * partition_scan_ratio * read_heaviness_boost
```

Where:
- **total_read_time** = `select_count * avg_query_duration_s` — total seconds spent reading the table in the lookback window
- **partition_scan_ratio** = `partitions_scanned / (partitions_scanned + partitions_pruned)` — fraction of micropartitions scanned per table from `TABLE_QUERY_PRUNING_HISTORY`. This is a **per-table** metric (not per-query). Falls back to 0.5 (neutral) when no pruning data is available.
- **read_heaviness_boost** = stepped multiplier (1-5x) based on `query_to_dml_ratio`: ratio >= 20 = 5x, >= 10 = 4x, >= 5 = 3x, >= 2 = 2x, else 1x

### Why each component

| Factor | What it captures | Why it belongs |
|--------|-----------------|----------------|
| `total_read_time` | Absolute pain: how many seconds were spent reading this table | Tables nobody queries don't need clustering regardless of condition |
| `partition_scan_ratio` | Scan efficiency: what fraction of the table's partitions are scanned | If queries already prune to 5% of partitions, clustering won't help. If they scan 80%, clustering has huge upside. **Per-table metric from TABLE_QUERY_PRUNING_HISTORY** — not diluted by other tables in multi-table queries. |
| `read_heaviness_boost` | ROI multiplier: read-heavy tables recoup clustering cost faster | A 100:1 read/write table amortizes reclustering cost over 100 reads. Stepped tiers keep the boost bounded. |

### Properties

- **Warehouse-size independent** — `partition_scan_ratio` measures physical scan behavior, not wall-clock time
- **Bounded** — no component is unbounded (scan ratio is 0-1, read_heaviness is log-bounded)
- **Explainable** — the `recommendation_reason` column templates the score components into natural language

### Scoring evolution

| Version | Formula | Rationale | Status |
|---------|---------|-----------|--------|
| V1 | `(select_count * avg_exec_sec) + (query_ratio * 10)` multiplied by `partition_ratio_pct` | First attempt: combined signals additively + unbounded multiplier | Historical (`find_table_clustering_candidates.sql`) |
| V2 | `select_count * avg_exec_sec` | Simplified: removed invisible additive term and unbounded multiplier; surfaced signals as display metrics only | Historical (`find_table_clustering_candidates_v2.sql`) |
| V3 | `total_read_time * scan_ratio * stepped_read_boost` | Best of both: retains scan inefficiency signal (bounded 0-1), adds read-heaviness as a stepped multiplier, warehouse-size independent, uses per-table pruning data from TABLE_QUERY_PRUNING_HISTORY | Current (fact model + `find_table_clustering_candidates.sql`) |

V1's issues: the additive `query_ratio * 10` was orders of magnitude smaller than the first term in practice (invisible in ranking). The partition_ratio_pct multiplier was unbounded and could produce extreme scores for fragmented but unqueried tables.

V2's issues: lost the scan efficiency signal entirely — a table scanned at 90% of partitions ranked the same as one scanned at 5% if query counts and durations were similar. Also not warehouse-size independent (upscaled compute masks slow scans).

V3 addresses both: the scan ratio directly measures pruning effectiveness (independent of warehouse size), and the read-heaviness boost is meaningful but bounded.

### Scoring divergence: DAG vs macro

The V3 scoring formula is implemented in two places with slightly different `read_heaviness_boost` calculations:

| Implementation | read_heaviness_boost | Why |
|---------------|---------------------|-----|
| **DAG** (`fct_snowflake__table_clustering_candidates`) | `1 + log(2, query_to_dml_ratio + 1)` | SQL-native, continuous, produces smooth rankings across the full ratio spectrum |
| **Macro** (`find_table_clustering_candidates`) | Stepped multiplier: ratio >= 20 = 5x, >= 10 = 4x, >= 5 = 3x, >= 2 = 2x, else 1x | Fusion-safe (avoids Jinja `| log` on query results), discrete tiers |

Both produce the same directional rankings — tables with higher read:write ratios score higher. However, the absolute score magnitudes will not match exactly between a macro run and the fact model for the same table.

**Use the DAG** for persistent daily monitoring, trend analysis, and dashboards.
**Use the macro** for one-off interactive checks and quick assessments.

---

## Understanding `is_candidate` and `recommendation_tier`

The model outputs both a boolean `is_candidate` and a descriptive `recommendation_tier`.

### recommendation_tier

| Tier | Meaning | Action |
|------|---------|--------|
| **High impact** | scan_ratio >= 0.5 AND score >= 1000 | Strong clustering candidate. Run `suggest_clustering_keys` next. |
| **Moderate impact** | scan_ratio >= 0.5 AND score < 1000 | Clustering would help but overall query load is moderate. |
| **Low impact** | scan_ratio 0.2 - 0.5 | Some pruning inefficiency but not severe. Monitor. |
| **Healthy** | scan_ratio < 0.2 | Pruning is effective. No clustering needed. |
| **Write-heavy** | query_to_dml_ratio <= 1 | More writes than reads — clustering ROI unclear. |
| **No read activity** | select_count = 0 | Table isn't being queried in the lookback window. |

### is_candidate (boolean)

A table is flagged as `is_candidate = true` only when **all four** conditions are met:

1. **The table is actively read.** `select_count > 0` — at least one SELECT query was executed against the table during the lookback window.

2. **Reads outnumber writes.** `query_to_dml_ratio > 1` — the table receives more SELECT queries than DML operations (INSERT, UPDATE, DELETE, MERGE). Clustering reorganizes data on disk to improve read performance, but every write operation can disrupt that organization. Tables with heavy write activity will see clustering benefits eroded quickly, increasing automatic reclustering costs.

3. **The table meets the minimum size threshold.** `table_size_gb >= clustering_candidates_min_size_gb` — clustering overhead (compute cost for automatic reclustering, metadata management) is typically not justified for small tables. The default threshold is 100 GB.

4. **Poor partition pruning.** `partition_scan_ratio > 0.5` — queries scan more than half of all micropartitions on average, indicating that Snowflake is not effectively pruning partitions for this table's query patterns. This is the signal that clustering would directly improve.

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

### Top clustering candidates with explanations

```sql
select
    table_fqn,
    dbt_model,
    score,
    is_candidate,
    recommendation_reason,
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

## Data Sources and Metric Design

### Partition scan ratio: source and grain

The `scan_ratio` (and `scan_ratio_pct`) metric comes from `SNOWFLAKE.ACCOUNT_USAGE.TABLE_QUERY_PRUNING_HISTORY`.

**Why this view (not QUERY_HISTORY):**

`QUERY_HISTORY.partitions_scanned` and `partitions_total` are *query-level* stats — they report totals across ALL tables touched by a statement. For any multi-table query (joins, subqueries), these numbers are meaningless for evaluating a single table's pruning efficiency. `TABLE_QUERY_PRUNING_HISTORY` provides per-table partition stats, which is what clustering evaluation actually needs.

**Source grain:** One row per `(table_id, warehouse_id, query_hash, hourly_interval)`. Each row contains:
- `num_queries` — how many times that query pattern ran against that table in that hour
- `partitions_scanned` — micropartitions scanned for this specific table
- `partitions_pruned` — micropartitions pruned (skipped) for this specific table

**Aggregation path:**

```
TABLE_QUERY_PRUNING_HISTORY (hourly, per table/warehouse/query_hash)
    ↓ stg_snowflake__table_query_pruning_history (preserves source grain)
    ↓ int_snowflake__table_query_stats_daily (aggregated to: one row per table_fqn per day)
    ↓ fct_snowflake__table_clustering_candidates (summed across lookback_days → one row per table per snapshot)
```

### Join key strategy

| Join point | Keys | Method |
|-----------|------|--------|
| Pruning history → candidate tables | `database_name`, `schema_name`, `table_name` | Exact match (UPPER on both sides) |
| Pruning history → daily grain | `interval_start_time::date = stats_date` | Date cast |
| Daily stats → fact model | `table_database`, `table_schema`, `table_name` + date range | Exact match |
| Fact model → dbt models | `database_name`, `schema_name`, `table_name` | Via `int_dbt__relations` |

No fuzzy `ILIKE` matching is used in the pruning path. The pruning view provides exact database/schema/table identifiers.

### Metric: scan_ratio

```sql
scan_ratio = SUM(partitions_scanned) / NULLIF(SUM(partitions_scanned + partitions_pruned), 0)
```

This is a **query-volume-weighted** metric, not a simple average. A table that receives 1000 queries in the window contributes proportionally more partition data than one that receives 10. This is intentional — it measures the actual proportion of partition I/O that is wasteful across the full workload, not per-query.

| scan_ratio | Interpretation |
|-----------|----------------|
| 1.0 (100%) | Every partition is scanned on every query — no pruning at all |
| 0.5 (50%) | Half of partitions scanned — moderate pruning |
| 0.05 (5%) | Excellent pruning — only 5% of partitions touched |
| NULL / fallback 0.5 | No pruning data available (see below) |

### Fallback behavior (scan_ratio = 0.5)

The fallback fires when `SUM(partitions_scanned + partitions_pruned) = 0` for a table in the lookback window. This happens when:

1. **No queries with WHERE/JOIN predicates ran against the table.** The pruning view only records rows when Snowflake evaluates a pruning decision. Full-table scans without any filter (`SELECT * FROM table`) don't appear.
2. **The pruning data hasn't populated yet.** Latency is up to 4 hours.
3. **The table was recently created** and hasn't accumulated enough query history.

When the fallback fires:
- The `recommendation_tier` will show based on the 0.5 ratio (typically "Moderate impact" for read-heavy tables)
- The `recommendation_reason` will show "Below size threshold or insufficient data"
- This is a **conservative neutral** — it doesn't claim pruning is good or bad, it flags the table for investigation

### Edition and availability

`TABLE_QUERY_PRUNING_HISTORY` is available to the `USAGE_VIEWER` database role — the same access level as all other ACCOUNT_USAGE views. No Snowflake edition restriction is documented. The base grant `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` is sufficient.

Latency: up to 4 hours (longest of the ACCOUNT_USAGE views used by this package).

---

## Notes

- **Snowflake edition:** Enterprise edition (or higher) is recommended for query-to-table attribution (via ACCESS_HISTORY). Standard edition falls back to `query_text` matching for DML counts. The partition scan ratio (from TABLE_QUERY_PRUNING_HISTORY) works on all editions.
- **Account usage latency:** TABLE_QUERY_PRUNING_HISTORY can lag by up to 4 hours. Other views lag 45 minutes to 3 hours. Run the pipeline after these windows for complete results.
- **First build performance:** The first run processes the full initial lookback window (default 30 days for Enterprise, 7 days for Standard). Subsequent incremental runs only process new data and are significantly faster.
- **Incremental snapshots:** The fact model produces one snapshot per day. Running it multiple times in the same day updates (merges) the existing snapshot for that date rather than creating duplicates.
