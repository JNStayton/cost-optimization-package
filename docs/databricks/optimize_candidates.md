# OPTIMIZE Candidates

This document covers the pipeline for identifying Delta tables that have accumulated file fragmentation and would benefit from running `OPTIMIZE` — either manually or by enabling Predictive Optimization.

## Pipeline Overview

```
Staging                              Intermediate                               Mart
───────                              ────────────                               ────
stg_databricks__table_info       ─┐
  (system.storage.                ├──► int_databricks__table_storage  ─┐
   table_metrics_history)         │                                     │
                                  └──► int_databricks__tables         ─├──► int_databricks__table_inventory ──► fct_databricks__
                                                                        │                                        optimize_candidates
stg_databricks__table_info       ──────────────────────────────────────┘ (predictive_optimization_enabled)
  (direct join for PO flag)

                                  int_dbt__relations                  ──────────────────────────────────────────┘
```

### Staging layer

| Model | Source | Materialization |
|-------|--------|-----------------|
| `stg_databricks__table_info` | `system.storage.table_metrics_history` | view (latest snapshot per table) |

### Intermediate layer

| Model | Purpose |
|-------|---------|
| `int_databricks__table_inventory` | Joins table metadata with storage metrics. Produces one row per active, non-deleted table with size, file count, and clustering state. |
| `int_dbt__relations` | Compile-time mapping from dbt model metadata to physical relation names. Used to attribute results back to dbt models. |

### Fact model

**`fct_databricks__optimize_candidates`** identifies tables with high file fragmentation (many small files) where Predictive Optimization is not already enabled, and recommends the appropriate remediation action.

---

## What is file fragmentation and why does it matter?

Every write to a Delta table — an INSERT, UPDATE, DELETE, or MERGE — produces new files. Over time, a frequently-written table accumulates hundreds or thousands of small files. When Databricks runs a query against a fragmented table, it must open and read many files instead of a few large ones, increasing I/O, memory pressure, and query cost.

`OPTIMIZE` compacts those small files into larger ones (targeting ~128 MB by default). After compaction, queries scan fewer files and run faster at lower cost.

**Two ways to keep a table compacted:**

1. **Run `OPTIMIZE` manually** on a schedule (e.g. after a nightly load):
   ```sql
   OPTIMIZE catalog.schema.my_table;
   ```

2. **Enable Predictive Optimization** — Databricks automatically determines when a table needs `OPTIMIZE` and runs it in the background. No scheduling required. Available on Unity Catalog with DBU cost.

This model surfaces tables that need attention and recommends which approach to take.

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

| Variable | Default | Description |
|----------|---------|-------------|
| `optimize_candidates_min_file_count` | `50` | Minimum number of active files for a table to be evaluated. Tables with fewer files are not significantly fragmented. |
| `optimize_candidates_max_avg_file_size_mb` | `128` | Tables with an average file size below this threshold are considered fragmented. 128 MB is the Delta Lake recommended target file size. |
| `optimize_candidates_min_size_gb` | `0.1` | Minimum table size in GB to evaluate. |
| `optimize_candidates_dbt_project_only` | `true` | When `true`, only tables that match a dbt model in the current project are included. Set to `false` to evaluate all tables meeting the thresholds. |
| `optimize_candidates_target_databases` | `[]` | Optional list of catalog names to restrict evaluation to. |
| `optimize_candidates_target_schemas` | `[]` | Optional list of schema names to restrict evaluation to. |

### Example configuration

```yaml
# dbt_project.yml
vars:
  # Include all tables, not just dbt models
  optimize_candidates_dbt_project_only: false

  # Lower file count threshold for smaller tables
  optimize_candidates_min_file_count: 20

  # Target a specific catalog
  optimize_candidates_target_databases: ["my_catalog"]
```

---

## Output Columns

| Column | Description |
|--------|-------------|
| `snapshot_date` | Date of the snapshot (one per day) |
| `database_name` | Catalog containing the table |
| `schema_name` | Schema containing the table |
| `table_name` | Table name |
| `table_fqn` | Fully qualified name (`catalog.schema.table`) |
| `dbt_model` | dbt `unique_id` if the table is a dbt model, otherwise `null` |
| `table_type` | Managed Table or External Table |
| `table_size_gb` | Table size in GB (active bytes only) |
| `current_file_count` | Number of active Delta files in the table |
| `avg_file_size_mb` | Average file size in MB. Lower = more fragmented. |
| `is_already_clustered` | Whether the table has Liquid Clustering or partition columns defined |
| `predictive_optimization_enabled` | Whether Databricks Predictive Optimization is already managing this table |
| `recommended_action` | `RUN OPTIMIZE` or `ENABLE PREDICTIVE OPTIMIZATION` (see below) |
| `score` | `file_count × (max_avg_file_size_mb / avg_file_size_mb)` — higher means more fragmented relative to target |
| `is_candidate` | `true` when the table is fragmented and Predictive Optimization is not already enabled |

---

## Understanding `recommended_action`

| Value | Meaning |
|-------|---------|
| `RUN OPTIMIZE` | Table is fragmented. Run `OPTIMIZE catalog.schema.table` to compact files. Consider scheduling this after regular write operations. |
| `ENABLE PREDICTIVE OPTIMIZATION` | Table already has Liquid Clustering defined. Enabling Predictive Optimization will automatically run `OPTIMIZE` when needed, maintaining the cluster layout without manual scheduling. |

**Note:** If `predictive_optimization_enabled = true`, the table is excluded from results entirely — Databricks is already managing compaction automatically.

### Enabling Predictive Optimization

The `predictive_optimization_enabled` flag comes from `system.storage.table_metrics_history` — it reflects whether PO is active on the table in Databricks. It is not set in dbt. You can enable it:

- **Per table** via SQL:
  ```sql
  ALTER TABLE catalog.schema.my_table
  SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'enable');
  ```
- **At the catalog or schema level** in the Databricks Unity Catalog UI (requires account admin).

Once enabled, the table will no longer appear in this model's results. Predictive Optimization and Liquid Clustering are complementary — enabling PO on a table that also has Liquid Clustering defined is the most hands-off approach, as Databricks will automatically run `OPTIMIZE` to both compact files and maintain the cluster layout.

---

## Sample Queries

### Most fragmented tables

```sql
select
    table_fqn,
    dbt_model,
    table_size_gb,
    current_file_count,
    avg_file_size_mb,
    is_already_clustered,
    recommended_action,
    score,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__optimize_candidates
where snapshot_date = current_date()
order by score desc;
```

### Tables to run OPTIMIZE on immediately

```sql
select
    table_fqn,
    current_file_count,
    avg_file_size_mb,
    table_size_gb,
    'OPTIMIZE ' || table_fqn || ';' as optimize_statement
from <your_catalog>.<your_schema>.fct_databricks__optimize_candidates
where snapshot_date = current_date()
    and is_candidate = true
    and recommended_action = 'RUN OPTIMIZE'
order by score desc;
```

### Historical fragmentation trend for a specific table

```sql
select
    snapshot_date,
    current_file_count,
    avg_file_size_mb,
    score,
    is_candidate
from <your_catalog>.<your_schema>.fct_databricks__optimize_candidates
where table_fqn = 'my_catalog.my_schema.my_table'
order by snapshot_date;
```

---

## Notes

- **`system.storage.table_metrics_history` must be enabled.** An account admin must enable the `system.storage` schema before data appears. The table is populated once per day — allow up to 24 hours after enablement for the first snapshot.
- **Predictive Optimization availability.** Predictive Optimization is available for Unity Catalog managed tables on Databricks. It is not available for external tables or tables in the Hive metastore. Check your workspace tier for availability.
- **External tables.** `OPTIMIZE` can be run on external Delta tables, but Predictive Optimization is only available for managed tables. External tables will always show `RUN OPTIMIZE` as the recommended action.
- **Liquid Clustering and OPTIMIZE.** Tables with Liquid Clustering enabled use `OPTIMIZE` to maintain the cluster layout — running `OPTIMIZE` on a clustered table both compacts files and re-clusters data. Enabling Predictive Optimization on a clustered table is the most hands-off approach. See the [Optimize Candidates vs. Liquid Clustering Candidates](#optimize-candidates-vs-liquid-clustering-candidates) section above for a full comparison of the two models.
- **Incremental snapshots.** The fact model produces one snapshot per day. Running it multiple times in the same day merges into the existing snapshot rather than creating duplicates.
