# Standalone Macros

These macros run independently of the model pipeline via `dbt run-operation`. They query Snowflake ACCOUNT_USAGE directly and print results to the console — no models need to be built first.

---

## find_table_clustering_candidates

Identifies tables that may benefit from clustering based on scan ratio, query volume, and read/write ratio.

```bash
dbt run-operation find_table_clustering_candidates
dbt run-operation find_table_clustering_candidates --args '{lookback_days: 14, ignore_table_size: true}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 7 | Days of query history to analyze |
| `ignore_table_size` | false | Skip the 1 GB minimum size filter |
| `dbt_project_only` | true | Only evaluate tables that are dbt models in the current project |
| `target_databases` | [] | Limit scan to specific databases |
| `target_schemas` | [] | Limit scan to specific schemas |

---

## suggest_clustering_keys

Suggests clustering key columns for a specific model based on filter/join usage from query operator stats and column cardinality.

```bash
dbt run-operation suggest_clustering_keys --args '{model_name: fct_order_items}'
dbt run-operation suggest_clustering_keys --args '{model_name: FCT_ORDER_ITEMS, database: MY_DB, schema: PROD_MARTS}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `model_name` | (required) | Name of the dbt model or table to analyze |
| `database` | none | Override database (bypasses ref, targets specific relation) |
| `schema` | none | Override schema (must be provided with database) |
| `include_boolean_cols` | false | Include low-cardinality columns (>= 2 distinct) as candidates |

---

## find_table_materialization_candidates

Identifies views with high query volume that should be materialized as tables.

```bash
dbt run-operation find_table_materialization_candidates
dbt run-operation find_table_materialization_candidates --args '{lookback_days: 30, min_query_count: 5}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 14 | Days of query history to analyze |
| `min_query_count` | 10 | Minimum queries in the window to surface a view |

---

## find_incremental_materialization_candidates

Identifies table models with slow, expensive builds that would benefit from incremental materialization.

```bash
dbt run-operation find_incremental_materialization_candidates
dbt run-operation find_incremental_materialization_candidates --args '{min_table_size_gb: 0, max_build_time_sec: 10}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `min_table_size_gb` | 10 | Minimum table size to consider |
| `max_build_time_sec` | 600 | Minimum build time (seconds) to flag |
| `lookback_days` | 30 | Days of build history to analyze |

---

## find_expensive_dbt_queries

Surfaces the most expensive recurring dbt queries by projected annual credit cost.

```bash
dbt run-operation find_expensive_dbt_queries
dbt run-operation find_expensive_dbt_queries --args '{lookback_days: 14, dbt_project_only: false}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 7 | Days of query history to analyze |
| `top_n` | 20 | Maximum number of queries to surface |
| `min_total_credits` | 0.1 | Minimum credits consumed in the window |
| `credit_rate_usd` | 2 | Dollar cost per Snowflake credit |
| `high_cost_threshold_usd` | 10000 | Threshold for "high cost" warning tier |
| `dbt_project_only` | true | Only show queries belonging to models in the current project |

---

## find_spillage_candidates

Finds dbt models with query spillage (local or remote) indicating undersized warehouses or inefficient SQL.

```bash
dbt run-operation find_spillage_candidates
dbt run-operation find_spillage_candidates --args '{lookback_days: 14, min_total_gb_spilled: 1}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 7 | Days of query history to analyze |
| `min_total_gb_spilled` | 0.05 | Minimum total GB spilled to surface |
| `min_runs` | 1 | Minimum query runs with spillage |

---

## find_warehouse_sizing_recommendations

Analyzes warehouse performance and recommends sizing changes (scale up/down, Gen2, MCW).

```bash
dbt run-operation find_warehouse_sizing_recommendations
dbt run-operation find_warehouse_sizing_recommendations --args '{lookback_days: 14}'
```

| Argument | Default | Description |
|----------|---------|-------------|
| `lookback_days` | 7 | Days of warehouse history to analyze |
| `min_query_count` | 20 | Minimum queries on a warehouse to evaluate |
| `dml_threshold` | 0.35 | DML ratio above which a warehouse is considered write-heavy |

---

## Notes

- All macros require `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database (ACCOUNT_USAGE access).
- Macros respect `snowflake_enterprise_edition` var — Standard edition uses query_text matching instead of ACCESS_HISTORY where applicable.
- `suppress_staging_materialization_recs` var is respected by `find_table_materialization_candidates`.
- For full analysis with strategy recommendations, confidence scores, and template code, run the model pipeline instead (`dbt run -s tag:dbt_cost_optimization`).
