# dbt Cost Optimization Package

A dbt package that provides models and macros to analyze your compute and storage spend, identify optimization opportunities, and produce actionable recommendations.

## Supported Platforms

- Snowflake
- Databricks
- Redshift
- BigQuery

---

## Snowflake: Required Permissions

### Overview

This package reads from Snowflake's `ACCOUNT_USAGE` views to analyze query patterns, warehouse utilization, table structure, and AI/Cortex consumption. The role used by dbt needs specific grants to access these views and to perform deep analysis on your project's tables.

### Minimum Setup (monitoring + recommendations)

Run the following as `ACCOUNTADMIN` or a role with `MANAGE GRANTS`:

```sql
-- =============================================================================
-- STEP 1: Grant access to Snowflake's ACCOUNT_USAGE views
-- This is required for ALL package features.
-- =============================================================================

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_dbt_role>;

-- =============================================================================
-- STEP 2: Grant warehouse usage for macro execution and model builds
-- =============================================================================

GRANT USAGE ON WAREHOUSE <your_warehouse> TO ROLE <your_dbt_role>;

-- =============================================================================
-- STEP 3: Grant schema permissions for model materialization
-- The package materializes its output tables/views in a configurable schema
-- (default: <your_database>.dbt_cost_optimization).
-- =============================================================================

-- Option A: Let the role create the schema if it doesn't exist
GRANT CREATE SCHEMA ON DATABASE <your_database> TO ROLE <your_dbt_role>;

-- Option B: If the schema already exists, grant usage + create permissions
GRANT USAGE ON SCHEMA <your_database>.<output_schema> TO ROLE <your_dbt_role>;
GRANT CREATE TABLE ON SCHEMA <your_database>.<output_schema> TO ROLE <your_dbt_role>;
GRANT CREATE VIEW ON SCHEMA <your_database>.<output_schema> TO ROLE <your_dbt_role>;
```

### Full Setup (includes deep analysis features)

The clustering key analysis and incremental key probing features run `APPROX_COUNT_DISTINCT` directly against your project's tables to measure column cardinality. This requires SELECT on those tables.

```sql
-- =============================================================================
-- STEP 4 (optional): Grant SELECT on tables to be analyzed
-- Required for: suggest_clustering_keys macro, refresh_column_cardinality
-- post-hook, probe_unique_key_candidates post-hook.
--
-- If your dbt role already owns or can query the tables it builds, this is
-- already satisfied. Only needed if the package runs on a different role than
-- the one that built the models.
-- =============================================================================

-- Option A: Grant on all tables in a specific schema
GRANT SELECT ON ALL TABLES IN SCHEMA <database>.<schema> TO ROLE <your_dbt_role>;
GRANT SELECT ON FUTURE TABLES IN SCHEMA <database>.<schema> TO ROLE <your_dbt_role>;

-- Option B: Grant on all tables in a database
GRANT SELECT ON ALL TABLES IN DATABASE <database> TO ROLE <your_dbt_role>;
GRANT SELECT ON FUTURE TABLES IN DATABASE <database> TO ROLE <your_dbt_role>;

-- Option C: Grant on specific tables only
GRANT SELECT ON TABLE <database>.<schema>.<table> TO ROLE <your_dbt_role>;
```

### Complete Script (copy-paste ready)

Replace the placeholders and run as `ACCOUNTADMIN`:

```sql
-- ============================================================
-- dbt Cost Optimization Package — Snowflake Permissions Setup
-- ============================================================
-- Replace these values:
--   <DBT_ROLE>       = the role your dbt project uses (e.g., TRANSFORMER)
--   <WAREHOUSE>      = the warehouse dbt runs on (e.g., TRANSFORMING)
--   <DATABASE>       = the database where package output tables go
--   <OUTPUT_SCHEMA>  = the schema for output (default: dbt_cost_optimization)
--   <PROJECT_DB>     = the database containing tables you want to analyze
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- 1. Account Usage access (required)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <DBT_ROLE>;

-- 2. Warehouse usage (required)
GRANT USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE <DBT_ROLE>;

-- 3. Output schema (required)
GRANT USAGE ON DATABASE <DATABASE> TO ROLE <DBT_ROLE>;
GRANT CREATE SCHEMA ON DATABASE <DATABASE> TO ROLE <DBT_ROLE>;
-- Or if schema exists:
-- GRANT USAGE ON SCHEMA <DATABASE>.<OUTPUT_SCHEMA> TO ROLE <DBT_ROLE>;
-- GRANT CREATE TABLE ON SCHEMA <DATABASE>.<OUTPUT_SCHEMA> TO ROLE <DBT_ROLE>;
-- GRANT CREATE VIEW ON SCHEMA <DATABASE>.<OUTPUT_SCHEMA> TO ROLE <DBT_ROLE>;

-- 4. Table analysis (optional — for clustering key + unique key probing)
GRANT SELECT ON ALL TABLES IN DATABASE <PROJECT_DB> TO ROLE <DBT_ROLE>;
GRANT SELECT ON FUTURE TABLES IN DATABASE <PROJECT_DB> TO ROLE <DBT_ROLE>;
```

### Permission Matrix by Feature

| Feature | `IMPORTED PRIVILEGES` | Warehouse | Output Schema | SELECT on tables |
|---------|:---------------------:|:---------:|:-------------:|:----------------:|
| **Warehouse sizing recommendations** | Required | Required | Required | — |
| **Warehouse spillage recommendations** | Required | Required | Required | — |
| **Expensive query recommendations** | Required | Required | Required | — |
| **Table clustering candidates** | Required | Required | Required | — |
| **Clustering key candidates** | Required | Required | Required | Required |
| **Materialization candidates** | Required | Required | Required | — |
| **Incremental config recommendations** | Required | Required | Required | Required |
| **AI/Cortex spend overview** | Required | Required | Required | — |
| **AI model cost recommendations** | Required | Required | Required | — |
| **AI user spend recommendations** | Required | Required | Required | — |
| **AI token efficiency recommendations** | Required | Required | Required | — |
| **All `dbt run-operation` macros** | Required | Required | — | Some |

### Edition-Specific Features

Some package features use views that are only available on Enterprise edition or higher:

| View | Required By | Edition |
|------|-------------|---------|
| `ACCESS_HISTORY` | Table-level spillage attribution, precise query-to-table mapping | Enterprise+ |
| `QUERY_ATTRIBUTION_HISTORY` | Exact per-query credit attribution | Enterprise+ |

When these views are unavailable (Standard edition), the package gracefully falls back:
- Spillage recommendations produce no rows (with an explanatory message)
- Credit attribution uses elapsed-time proration (approximate, flagged in output)
- Query-to-table attribution falls back to `query_text ILIKE` matching

Set `use_access_history_attribution: false` in your `dbt_project.yml` if you're on Standard edition.

### ACCOUNT_USAGE Latency

Snowflake's ACCOUNT_USAGE views have data latency:

| View | Typical Latency | Max Latency |
|------|----------------|-------------|
| `QUERY_HISTORY` | 10-15 minutes | 45 minutes |
| `ACCESS_HISTORY` | 30-90 minutes | 3 hours |
| `TABLE_STORAGE_METRICS` | 30-90 minutes | 3 hours |
| `TABLE_QUERY_PRUNING_HISTORY` | 1-2 hours | 4 hours |
| `WAREHOUSE_METERING_HISTORY` | 30-90 minutes | 3 hours |
| `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` | 10-30 minutes | 60 minutes |

Schedule package builds after these latency windows for complete results. First builds on incremental models will backfill the configured lookback window (default 30 days for most features).

---

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/dbt-labs/dbt-cost-optimization-package.git"
    revision: main
```

Then run:

```bash
dbt deps
```

---

## Quick Start

### Run the models (persistent monitoring)

```bash
# Build all Snowflake optimization models
dbt run --select tag:clustering tag:materialization tag:warehouse tag:ai_spend

# Or run a specific optimization path
dbt run --select tag:warehouse
dbt run --select tag:clustering
dbt run --select tag:ai_spend
```

### Run a macro (one-off quick check)

```bash
# Find tables that should be clustered
dbt run-operation find_table_clustering_candidates_v3

# Find views that should be materialized
dbt run-operation find_table_materialization_candidates

# Find tables that should be incremental
dbt run-operation find_incremental_materialization_candidates

# Find expensive dbt queries
dbt run-operation find_expensive_dbt_queries

# Check warehouse sizing
dbt run-operation find_warehouse_sizing_recommendations

# Get clustering key suggestions for a specific model
dbt run-operation suggest_clustering_keys --args '{model_name: my_model}'
```

---

## Configuration

All package variables can be overridden in your `dbt_project.yml`:

```yaml
vars:
  # Edition settings
  use_access_history_attribution: true   # Set to false for Standard edition
  use_query_attribution: true            # Set to false for Standard edition

  # Clustering
  clustering_candidates_min_size_gb: 100
  clustering_candidates_lookback_days: 7

  # Warehouse
  warehouse_sizing_lookback_days: 30
  expensive_query_credit_rate_usd: 2     # Your contract rate

  # AI/Cortex
  ai_spend_lookback_days: 30
  ai_credit_rate_usd: 2
```

See the documentation in `docs/snowflake/` for the full variable reference for each optimization path.

---

## Documentation

Detailed documentation for each optimization path is in `docs/snowflake/`:

| Doc | Covers |
|-----|--------|
| [Table Clustering Candidates](docs/snowflake/table_clustering_candidates.md) | Clustering candidate scoring (V3), key recommendations |
| [Materialization Recommendations](docs/snowflake/materialization_recommendations.md) | View→table, table→incremental candidates |
| [Incremental Config Deep Dive](docs/snowflake/incremental_config_recommendations.md) | Strategy selection, key detection, implementation guide |
| [Warehouse Recommendations](docs/snowflake/warehouse_recommendations.md) | Sizing, spillage, expensive queries |
| [AI/Cortex Spend Optimization](docs/snowflake/ai_cortex_spend_optimization.md) | Token monitoring, model cost, user attribution |
| [Future Optimizations](docs/snowflake/future_optimizations.md) | Adaptive Compute, Gen2 changes, roadmap |
