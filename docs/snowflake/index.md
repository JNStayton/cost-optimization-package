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

Note: `QUERY_ATTRIBUTION_HISTORY` is available on all editions (data starts Aug 2024). The package automatically uses it when available and falls back to elapsed-time proration when a query has no attribution row.

When these views are unavailable (Standard edition), the package gracefully falls back:
- Spillage recommendations produce no rows (with an explanatory message)
- Query-to-table attribution falls back to `query_text ILIKE` matching

Set `snowflake_enterprise_edition: false` in your `dbt_project.yml` if you're on Standard edition.

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

## Package Models Are Disabled by Default

Persistent package models are **disabled by default** so they do not run on your project's normal `dbt run` or `dbt build`. Macros are always available regardless.

To build package models, explicitly opt in:

**Recommended: dedicated scheduled jobs** (no changes to existing jobs required)
```bash
# All package models
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select package:dbt_cost_optimization_package

# Or select by optimization domain
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:warehouse
```

**Alternative: enable at the project level** (builds on every dbt run)
```yaml
# In your dbt_project.yml vars: section OR in a vars.yml file
vars:
  dbt_cost_optimization_enabled: true
```
Note: if enabled at the project level, you will need to explicitly exclude package models from general runs where they are not desired.

### Suggested cadences

| Domain | Selector | Cadence | Why |
|--------|----------|---------|-----|
| Warehouse | `+tag:warehouse` | Weekly | Concurrency, spillage, and expensive queries shift quickly |
| AI / Cortex | `+tag:ai_spend` | Weekly | Token usage and user concentration can change fast |
| Materialization | `+tag:materialization` | Monthly | Benefits from accumulated build history |
| Clustering | `+tag:clustering` | Monthly | Pruning patterns are more meaningful over longer windows |

Example dedicated weekly job:
```bash
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:warehouse +tag:ai_spend
```

Example dedicated monthly job:
```bash
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:clustering +tag:materialization
```

The `+` prefix ensures upstream staging/intermediate dependencies are included.

---

## Quick Start

### Run a macro (one-off quick check)

Macros work immediately after `dbt deps` — no opt-in needed:

```bash
# Find tables that should be clustered
dbt run-operation find_table_clustering_candidates

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

All package variables can be overridden in any of the following ways:

- In a `vars.yml` file in your project root
- Via CLI: `--vars '{var_name: value}'`
- In a deployment job command

Note: overriding package vars in your `dbt_project.yml` `vars:` section is **not supported** due to parse-order limitations with package `+enabled` configs.

### Edition and shared settings

| Variable | Default | Description |
|----------|---------|-------------|
| `snowflake_enterprise_edition` | `true` | Set to `false` for Standard edition. Controls ACCESS_HISTORY availability and multi-cluster recommendations. |
| `table_query_stats_full_account` | `false` | When `true`, collects query stats for ALL tables in the account. When `false`, only dbt models in the current project. |

### Table clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `clustering_candidates_min_size_gb` | `100` | Minimum table size in GB to evaluate. Lower for dev environments. |
| `clustering_candidates_lookback_days` | `7` | Days of query stats to aggregate when scoring. |
| `clustering_candidates_dbt_project_only` | `true` | When `true`, only tables matching dbt models are included. |
| `clustering_candidates_target_databases` | `[]` | Restrict evaluation to specific databases. Empty = no restriction. |
| `clustering_candidates_target_schemas` | `[]` | Restrict evaluation to specific schemas. Empty = no restriction. |
| `clustering_key_cardinality_table_limit` | `10` | Max tables to evaluate for column-level clustering key recommendations. |
| `clustering_key_operator_analysis_table_limit` | `10` | Max tables to analyze with GET_QUERY_OPERATOR_STATS for filter/join evidence. |
| `clustering_key_operator_queries_per_table` | `20` | Representative queries to analyze per candidate table (one per parameterized hash). |

### Materialization

| Variable | Default | Description |
|----------|---------|-------------|
| `table_materialization_lookback_days` | `14` | Lookback window for view query history. |
| `table_materialization_min_query_count` | `10` | Minimum queries for a view to appear in results. |
| `incremental_overlap_days` | `31` | Re-scan window on all incremental runs (including first build). Set to at least your longest gap between package builds. Default supports monthly cadence. Reduce to 7 if running weekly. |
| `incremental_unique_key_probe_threshold` | `0.95` | Uniqueness ratio threshold for key candidate detection. |
| `incremental_candidates_lookback_days` | `60` | Lookback window for table rebuild history. |
| `incremental_candidates_min_build_time_sec` | `300` | Min max build time for build-time trigger. |
| `incremental_candidates_min_size_gb` | `2` | Min table size for size trigger. |
| `incremental_candidates_min_compute_waste_score` | `5` | Min waste score for compute-waste trigger. |
| `incremental_candidates_min_qualified_build_days` | `3` | Min CTAS days to trust growth signal. |
| `incremental_candidates_min_compute_waste_avg_build_sec` | `30` | Min avg build time alongside waste trigger. |
| `incremental_candidates_roi_high_build_time_sec` | `300` | Min avg build time (seconds) for 'high' ROI tier. |
| `incremental_candidates_roi_medium_build_time_sec` | `120` | Min avg build time (seconds) for 'medium' ROI tier. |
| `incremental_large_table_row_threshold` | `10000000` | Row count above which `delete+insert` preferred over `merge`. |
| `incremental_large_table_gb_threshold` | `10` | Size in GB above which `delete+insert` preferred over `merge`. |

### Warehouse optimization

| Variable | Default | Description |
|----------|---------|-------------|
| `warehouse_sizing_lookback_days` | `30` | Analysis window for sizing recommendations. |
| `warehouse_sizing_dml_threshold` | `0.35` | DML ratio above which Gen2 is recommended. |
| `warehouse_sizing_min_query_count` | `20` | Minimum dbt queries to evaluate a warehouse. |
| `spillage_lookback_days` | `30` | Analysis window for spillage recommendations. |
| `spillage_min_total_gb` | `0.05` | Minimum total spillage (GB) to appear in results. |
| `spillage_min_runs` | `1` | Minimum DML/CTAS runs to appear in results. |
| `expensive_query_lookback_days` | `30` | Analysis window for expensive queries. |
| `credit_rate_usd` | `2` | Credit-to-dollar conversion rate (your contract rate). Used across all domains for cost estimation. |
| `expensive_query_high_cost_threshold` | `10000` | Annual projected cost threshold for "High Cost" tier. |
| `expensive_query_min_total_credits` | `0.1` | Minimum credits consumed to appear in results. |
| `expensive_query_top_n` | `50` | Maximum rows in expensive query output. |

### AI/Cortex spend

| Variable | Default | Description |
|----------|---------|-------------|
| `ai_spend_lookback_days` | `30` | Analysis window for AI spend models. |
| `ai_credit_rate_usd` | `2` | Credit-to-dollar conversion rate for AI services. |
| `ai_model_downgrade_output_token_threshold` | `100` | Avg output tokens below which a cheaper model is suggested. |
| `ai_prompt_bloat_input_token_threshold` | `10000` | Avg input tokens above which prompt trimming is suggested. |
| `ai_batch_opportunity_min_daily_calls` | `100` | Min daily calls for batch processing recommendation. |
| `ai_user_spike_threshold_pct` | `200` | Percentage of baseline that triggers a user spike alert. |
| `ai_user_concentration_threshold_pct` | `50` | % of total spend from one user that triggers concentration alert. |
| `ai_min_credits_for_recommendation` | `1` | Min credits for a model to produce recommendations. |
| `ai_min_queries_for_recommendation` | `10` | Min queries for a model to produce recommendations. |
| `ai_rest_api_enabled` | `true` | Set to `false` to exclude REST API (dollar-billed) from AI analysis. |

### Cross-environment model discovery

| Variable | Default | Description |
|----------|---------|-------------|
| `dbt_monitored_projects` | `[]` | Which dbt projects to monitor. Default: root project only. Set `['*']` for all dbt projects in the account, or list specific project names for mesh. |
| `include_full_platform_insights` | `false` | Show account-wide non-dbt signals (all warehouses, CoCo, Intelligence, untracked users). |
| `dbt_relation_history_lookback_days` | `90` | How far back to scan QUERY_HISTORY for dbt build comments. |

### Example: Standard edition configuration

```yaml
vars:
  snowflake_enterprise_edition: false
  clustering_candidates_min_size_gb: 1    # Lower for testing
  warehouse_sizing_lookback_days: 14
  credit_rate_usd: 3.5    # Your contract rate
```

---

## Documentation

Detailed documentation for each optimization path:

| Doc | Covers |
|-----|--------|
| [Table Clustering Candidates](reference/table_clustering_candidates.md) | Clustering candidate scoring (V3), key recommendations |
| [Materialization Recommendations](reference/materialization_recommendations.md) | View→table, table→incremental candidates |
| [Incremental Config Deep Dive](reference/incremental_config_recommendations.md) | Strategy selection, confidence scoring, implementation guide |
| [Incremental Design](reference/incremental_recommendations_mapping.md) | Confidence system architecture, scoring model, expected outputs |
| [Warehouse Recommendations](reference/warehouse_recommendations.md) | Sizing, spillage, expensive queries |
| [Warehouse Signals](reference/warehouse_recommendations_mapping.md) | Signal inventory, interaction matrix, symptom classification |
| [Optimization Priorities](reference/optimization_priorities_mapping.md) | Per-entity priority system, hierarchy ranks, cascade behavior |
| [Gold Layer Design](reference/gold_design.md) | View specifications, cost estimation, cross-domain correlations |
| [AI/Cortex Spend Optimization](reference/ai_cortex_spend_optimization.md) | Token monitoring, model cost, user attribution |
| [Future Optimizations](reference/future_optimizations.md) | Adaptive Compute, Gen2, roadmap |
