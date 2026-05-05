# Liquid Clustering Candidates

This document covers the end-to-end pipeline for identifying Databricks Delta tables that may benefit from Liquid Clustering, from staging through the fact model, with emphasis on configuration and how to interpret the results.

## Pipeline Overview

```
Staging                              Intermediate                               Mart
───────                              ────────────                               ────
stg_databricks__tables           ─┐
                                   ├──► int_databricks__tables        ─┐
stg_databricks__table_info       ─┘                                    │
  (system.storage.                ──► int_databricks__table_storage  ──├──► int_databricks__table_inventory ─┐
   table_metrics_history)                                              ─┘                                     │
                                                                                                              ├──► fct_databricks__liquid_
stg_databricks__query_history    ──► int_databricks__query_history    ─┐                                      │      clustering_candidates
  (system.query.history)                                               ├──► int_databricks__table_query_  ───┘
                                                                       │      stats_daily
                                  int_dbt__relations                  ─┘

                                  int_dbt__relations                  ──────────────────────────────────────────┘
```

### Staging layer

| Model | Source | Materialization |
|-------|--------|-----------------|
| `stg_databricks__tables` | `system.information_schema.tables` | view |
| `stg_databricks__table_info` | `system.storage.table_metrics_history` | view (latest snapshot per table) |
| `stg_databricks__query_history` | `system.query.history` | incremental (7-day lookback) |

**Note:** `system.storage.table_metrics_history` is a daily snapshot table populated by Databricks once per day. It must be enabled by an account admin before data appears. See [Databricks system tables documentation](https://docs.databricks.com/en/admin/system-tables/index.html).

### Intermediate layer

| Model | Purpose |
|-------|---------|
| `int_databricks__tables` | Normalizes table metadata from `table_metrics_history`. Maps Unity Catalog table types to standard types and extracts partition columns as a clustering key proxy. |
| `int_databricks__table_storage` | Extracts storage metrics per table: active bytes, file count. Time travel and failsafe bytes are not applicable for Databricks and are nulled out. |
| `int_databricks__table_inventory` | Joins table metadata with storage metrics. Produces one row per active, non-deleted table with size, file count, and clustering state. |
| `int_databricks__table_query_stats_daily` | Daily aggregated query statistics per table. Attribution uses `statement_text` matching — joins query text against table names to count reads and writes per table per day. |
| `int_dbt__relations` | Compile-time mapping from dbt model metadata to physical relation names. Used to attribute results back to dbt models. |

Platform-agnostic routers (`int_table_inventory`, `int_table_query_stats_daily`) sit between the platform-specific intermediate models and the fact. The fact references these routers, not the Databricks-specific models directly.

### Fact model

**`fct_databricks__liquid_clustering_candidates`** is the final output. It is an incremental model (merge strategy, one snapshot per day) that scores Delta tables and flags Liquid Clustering candidates based on query activity, table size, and file fragmentation.

---

## What is Liquid Clustering?

Liquid Clustering is Delta Lake's adaptive data layout feature. Instead of requiring you to define partition columns upfront, it continuously reorganizes table files to cluster data by the most-queried columns. This reduces the number of files Databricks must open per query (file pruning), which directly lowers bytes scanned and compute cost.

A fragmented table — one with many small files — forces Databricks to open and partially read hundreds of files to return results. A well-clustered table with fewer, larger files reduces that to a handful.

Liquid Clustering replaces both traditional Hive-style partitioning and manual `ZORDER` for most use cases. It is maintained by running `OPTIMIZE` on the table, which Databricks can also do automatically via Predictive Optimization.

---

## Optimize Candidates vs. Liquid Clustering Candidates

Both models identify fragmented Delta tables, but they answer different questions and recommend different actions.

| | `fct_databricks__optimize_candidates` | `fct_databricks__liquid_clustering_candidates` |
|---|---|---|
| **Question it answers** | "Which fragmented tables need compacting now?" | "Which heavily-queried tables would scan fewer files with liquid clustering?" |
| **Signal used** | Storage only (file count, avg file size) | Storage + query history (reads vs. writes) |
| **Action recommended** | Run `OPTIMIZE` or enable Predictive Optimization | Add `CLUSTER BY` columns to the table |
| **Effect** | Compacts existing small files into larger ones | Reorganizes data layout so future queries skip more files |
| **PO interaction** | Tables with PO already enabled are excluded (`is_candidate = false`) | PO status is not checked — liquid clustering is complementary to PO |
| **Type of change** | Maintenance (periodic or automatic) | Architectural (one-time schema change) |

### When a table appears in both

A large, read-heavy, fragmented table with Predictive Optimization disabled will show up in both models. The recommended sequence is:

1. **Short-term:** Run `OPTIMIZE` to compact existing files and improve query performance immediately.
2. **Medium-term:** Enable Predictive Optimization so Databricks handles compaction automatically going forward.
3. **Longer-term:** Evaluate Liquid Clustering if the table has clear high-cardinality filter columns — clustering will further reduce files scanned per query beyond what compaction alone achieves.

### When a table appears in only one

- **Optimize only:** The table is fragmented but not actively queried (no SELECT history), or has more DML than SELECTs. Compaction is still worth doing to keep storage efficient.
- **Liquid clustering only:** The table already has well-sized files (above the fragmentation threshold) but is very query-heavy and large. The benefit here comes from data layout, not file count.

---

## Project Variables

Set these in your `dbt_project.yml` under `vars:` to customize behavior.

### Query stats scope

| Variable | Default | Description |
|----------|---------|-------------|
| `table_query_stats_full_account` | `false` | When `false` (default), only collects query stats for tables that are dbt models in the current project. Set to `true` to scan query history against all tables in the inventory. |
| `table_query_stats_initial_lookback_days` | `7` | Number of days of query history to process on the first build. Subsequent incremental runs pick up from the last processed date. |

### Fact model tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `cluster_key_suggestion_lookback_days` | `30` | Number of days of query history to scan when scoring column suggestions. A longer window gives more signal but increases compute. |
| `liquid_clustering_candidates_min_size_gb` | `1` | Minimum table size in GB to evaluate. Only tables at or above this threshold appear in results. |
| `liquid_clustering_candidates_lookback_days` | `7` | Number of days of daily query stats to aggregate when computing scores and metrics. |
| `liquid_clustering_candidates_dbt_project_only` | `true` | When `true`, only tables that match a dbt model in the current project are included in the output. Set to `false` to include all tables that meet the size threshold. |
| `liquid_clustering_candidates_target_databases` | `[]` | Optional list of catalog names to restrict evaluation to. Empty = no restriction. |
| `liquid_clustering_candidates_target_schemas` | `[]` | Optional list of schema names to restrict evaluation to. Empty = no restriction. |
| `liquid_clustering_candidates_max_avg_file_size_mb` | `128` | Tables with an average file size above this threshold are considered well-compacted and deprioritized. Default 128 MB is the Delta Lake recommended target file size. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  # Lower threshold for dev/sandbox environments
  liquid_clustering_candidates_min_size_gb: 0.1

  # Widen the analysis window
  liquid_clustering_candidates_lookback_days: 14

  # Include all tables, not just dbt models
  liquid_clustering_candidates_dbt_project_only: false
```

---

## Output Columns

| Column | Description |
|--------|-------------|
| `analyzed_at` | Timestamp when the snapshot was computed |
| `snapshot_date` | Date of the snapshot (one per day) |
| `database_name` | Catalog containing the table |
| `schema_name` | Schema containing the table |
| `table_name` | Table name |
| `table_fqn` | Fully qualified name (`catalog.schema.table`) |
| `dbt_model` | dbt `unique_id` if the table is a dbt model, otherwise `null` |
| `table_type` | Managed Table or External Table |
| `score` | Composite score combining query volume, execution time, read/write ratio, and file fragmentation. Higher = more potential benefit from clustering. |
| `is_candidate` | Whether the table meets all criteria for Liquid Clustering (see below) |
| `table_size_gb` | Table size in GB (active bytes only, excludes time travel and failsafe) |
| `total_rows` | Row count (null — not available from `table_metrics_history`; join to `information_schema` if needed) |
| `current_file_count` | Number of active Delta files in the table |
| `avg_file_size_mb` | Average file size in MB (`active_bytes / file_count / 1024^2`) |
| `is_already_clustered` | Whether the table already has a clustering key or partition column defined |
| `avg_files_scanned` | Average number of files scanned per SELECT query in the lookback window |
| `select_count` | Number of SELECT queries against the table in the lookback window |
| `dml_count` | Number of INSERT/UPDATE/DELETE/MERGE operations in the lookback window |
| `query_to_dml_ratio` | `select_count / (dml_count + 1)` — higher means more read-heavy |
| `avg_query_duration_s` | Average SELECT execution time in seconds |
| `suggested_cluster_key` | Best-guess column to use as the `CLUSTER BY` key (see below) |
| `suggested_cluster_key_confidence` | `HIGH`, `MEDIUM`, `LOW`, or `HEURISTIC` — how confident the suggestion is |

---

## Understanding `is_candidate`

A table is flagged as `is_candidate = true` only when **all four** conditions are met:

1. **The table is actively read.** `select_count > 0` — at least one SELECT query was executed against the table during the lookback window.

2. **Reads outnumber writes.** `query_to_dml_ratio > 1` — the table receives more SELECT queries than DML operations. Liquid Clustering reorganizes files during `OPTIMIZE`, but every write introduces new small files that undo that organization. Tables with constant heavy writes will require frequent OPTIMIZE runs, increasing compute cost.

3. **The table meets the minimum size threshold.** `table_size_gb >= liquid_clustering_candidates_min_size_gb` — clustering overhead is not justified for small tables. Default is 1 GB.

4. **The table is fragmented.** `avg_file_size_mb < liquid_clustering_candidates_max_avg_file_size_mb` — a table with large, well-sized files is already efficiently laid out. Default threshold is 128 MB (Delta Lake's recommended target).

### When `is_candidate` is `false` but the data is still useful

A table may have a **high score** but `is_candidate = false`. Score and is_candidate measure different things:

- **Score** = "How much query activity and file fragmentation exists?" — a raw signal of potential benefit.
- **is_candidate** = "Is clustering practically viable given the read/write mix, size, and current file layout?"

A table that writes frequently but has very high query volume may still benefit from Liquid Clustering with Predictive Optimization enabled, which handles the OPTIMIZE cadence automatically.

---

## Understanding `suggested_cluster_key`

The `suggested_cluster_key` column provides a best-guess column name to use when running `ALTER TABLE ... CLUSTER BY`. It is generated by `int_databricks__column_cluster_suggestions`, which scores every column in the table using two signals:

### Why Databricks suggestions are approximate (vs. Snowflake)

On Snowflake, this package can identify the exact columns that were accessed in each query using `ACCESS_HISTORY` — a system table that records every column touched by every query, tagged as a filter, a projection, or a join condition. This means the Snowflake equivalent of this feature can tell you with certainty: "this column was used as a filter in 87 of the last 100 queries against this table."

**Databricks does not have an equivalent of `ACCESS_HISTORY`.** The closest system table is `system.query.history`, which records the raw SQL text of each query but does not parse or attribute column access. There is no native way to know which columns were in a `WHERE` clause vs. a `SELECT` list vs. a `JOIN` condition.

To work around this, `int_databricks__column_cluster_suggestions` uses **text matching**: it checks whether a column name appears anywhere in the query text (`ilike '%column_name%'`). If `created_at` appears in 80% of the SELECT queries that touch your table, it is likely being filtered on — but the model cannot be certain. It is co-occurrence, not attribution.

This means:
- The suggestion is a **starting point**, not a definitive answer.
- Confidence levels (`HIGH`, `MEDIUM`, `LOW`) reflect how often a column co-occurs in query text — not how often it is actually filtered on.
- The **`HEURISTIC`** confidence level is the fallback when no query history exists at all, using only data type and column name patterns.

Always validate the suggestion against your actual query patterns before applying `CLUSTER BY`.

---

**1. Query frequency (primary signal)**
The model scans query history for SELECT queries that mention the table (via `statement_text` matching) and counts how often each column name also appears in those queries. Columns that show up in many queries are more likely to be filter predicates and therefore good clustering candidates.

**2. Data type and name heuristics (fallback signal)**
When query history is unavailable or sparse, the model falls back to:
- **Data type:** `DATE` and `TIMESTAMP` columns receive a score boost — date-range filters are by far the most common clustering use case in Delta Lake.
- **Column name patterns:** names containing words like `date`, `_at`, `region`, `status`, `type`, `category` receive a smaller boost, as these are common filter column naming conventions.

### Confidence levels

| Value | Meaning |
|-------|---------|
| `HIGH` | Column appears in ≥ 50% of recent queries against this table |
| `MEDIUM` | Column appears in 20–49% of recent queries |
| `LOW` | Column appears in < 20% of recent queries, but still scored above alternatives |
| `HEURISTIC` | No query history exists for this table — suggestion is based on data type and column name only |
| `null` | No suggestion could be made (table has no columns that scored above zero) |

### Limitations

These limitations follow directly from Databricks's lack of column-level access attribution (see above):

- **Substring matching:** column names are matched against full query text using `ilike '%column_name%'`, so a column named `user_id` will also match queries mentioning `session_user_id`. Short or generic column names (under 4 characters) are excluded from scoring to reduce false positives, but some noise remains.
- **No WHERE clause parsing:** the model cannot distinguish a column used in a filter from one used in a SELECT list or JOIN condition. It uses co-occurrence in the query text as a proxy.
- **Redacted query text:** if your workspace uses customer-managed keys, `statement_text` is empty and the suggestion will fall back to `HEURISTIC` for all tables. This is a known Databricks limitation — the raw query text is the only available attribution signal, so redaction eliminates it entirely.
- **Treat as a starting point:** always validate the suggestion against your actual query patterns before applying `CLUSTER BY`. The model points you in the right direction; it doesn't replace engineering judgment.

### Applying the suggestion

```sql
-- Preview the recommendation
select
    table_fqn,
    suggested_cluster_key,
    suggested_cluster_key_confidence,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__liquid_clustering_candidates
where snapshot_date = current_date()
    and is_candidate = true
order by score desc;

-- Apply liquid clustering once you've validated the column
ALTER TABLE my_catalog.my_schema.my_table
CLUSTER BY (suggested_column_name);
```

---

## Sample Queries

### Top candidates by score

```sql
select
    table_fqn,
    dbt_model,
    score,
    is_candidate,
    table_size_gb,
    current_file_count,
    avg_file_size_mb,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_query_duration_s
from <your_catalog>.<your_schema>.fct_databricks__liquid_clustering_candidates
where snapshot_date = current_date()
order by score desc;
```

### Fragmented tables with high query activity

Tables where queries scan many files are the most likely to see immediate improvement.

```sql
select
    table_fqn,
    dbt_model,
    current_file_count,
    avg_file_size_mb,
    avg_files_scanned,
    select_count,
    avg_query_duration_s,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__liquid_clustering_candidates
where snapshot_date = current_date()
    and select_count > 0
order by avg_files_scanned desc;
```

### Historical trend for a specific table

```sql
select
    snapshot_date,
    score,
    is_candidate,
    current_file_count,
    avg_file_size_mb,
    select_count,
    avg_query_duration_s
from <your_catalog>.<your_schema>.fct_databricks__liquid_clustering_candidates
where table_fqn = 'my_catalog.my_schema.my_table'
order by snapshot_date;
```

---

## Notes

- **system.storage enablement:** `system.storage.table_metrics_history` must be explicitly enabled by a Databricks account admin before data appears. The table is populated once per day — allow up to 24 hours after enablement for the first snapshot.
- **statement_text redaction:** If your workspace uses customer-managed keys, `statement_text` in `system.query.history` will be empty. This means query-to-table attribution via text matching will not work and `select_count`/`dml_count` will be 0 for all tables. The storage-based columns (`file_count`, `avg_file_size_mb`, `size_gb`) are unaffected.
- **Query attribution approach:** Unlike Snowflake's ACCESS_HISTORY (which provides exact table-level attribution), Databricks query-to-table matching uses `statement_text ilike '%table_name%'`. This is approximate — it may miss queries that reference tables by alias only, and may over-count if a table name appears as a substring of another.
- **Partitioning vs Liquid Clustering:** Liquid Clustering replaces traditional Hive-style partitioning for Delta tables. If a table has existing partition columns, `is_already_clustered` will be `true` and it will not appear as a candidate unless you explicitly evaluate it.
- **Incremental snapshots:** The fact model produces one snapshot per day. Running it multiple times in the same day merges into the existing snapshot for that date rather than creating duplicates.
