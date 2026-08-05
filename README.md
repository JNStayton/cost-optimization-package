# dbt Cost Optimization Package

A dbt package that analyzes your data platform's compute, storage, and query patterns to identify optimization opportunities and produce actionable recommendations.

## Supported Platforms

| Platform | Status | Documentation |
|----------|--------|---------------|
| **Snowflake** | Active | [docs/snowflake/index.md](docs/snowflake/index.md) |
| **Redshift** | In development | [docs/redshift/](docs/redshift/) |
| **BigQuery** | In development | — |
| **Databricks** | Planned | — |

## What You'll Get

The package produces **gold-layer views** — dashboard-ready outputs that surface ranked, prioritized recommendations:

| View | Audience | What it shows |
|------|----------|--------------|
| `vw_snowflake__top_recommendations` | Executives / leads | Top-priority recommendations across all domains, deduped per entity |
| `vw_snowflake__dbt_model_optimizations` | dbt engineers | Actionable model changes: clustering keys, materialization, incremental configs |
| `vw_snowflake__warehouse_optimizations` | Snowflake admins | Warehouse config changes: auto-suspend, scaling, sizing — with linked model context |
| `vw_snowflake__optimization_backlog` | Sprint planning / agents | Full inventory of all signals (all priority tiers) for ticket creation |
| `vw_snowflake__top_expensive_queries` | Cost owners | Top 10 expensive queries enriched with root-cause co-signals |
| `vw_snowflake__top_spillage_models` | Performance engineers | Models causing the most memory spillage, with dbt Cloud run traceability |
| `vw_snowflake__top_queried_models` | Platform engineers | Most-queried models (downstream consumption pressure) |
| `vw_snowflake__cross_domain_insights` | Architecture leads | Multi-signal correlation (why issues co-occur on the same model) |
| `vw_snowflake__cost_savings_summary` | Dashboards | KPI tiles: total opportunity per domain |
| `vw_snowflake__ai_optimizations` | AI/ML teams | Cortex model cost, token efficiency, agent spend |

### Priority System

Each recommendation includes a `priority_tier` — a per-entity relative ordering:
- **P1** = do this first (highest in the optimization hierarchy for this model/warehouse)
- **P2** = do this second (after P1 is applied)
- **P3+** = deferred (waiting for higher-priority fixes to resolve the symptom)

Priority cascades naturally: when you apply a P1 fix (e.g., add a clustering key) and rebuild, the signal disappears and P2 promotes to P1 automatically.

---

## Scope

The package monitors:

| What | Scope | Notes |
|------|-------|-------|
| Model-level recommendations (clustering, materialization, incremental) | **This project only** | Requires dbt graph context (model configs, lineage) |
| Warehouse-level recommendations (config changes, spillage, idle credits) | **Warehouses used by this project** | Surfaces for any warehouse that runs project models |
| Spillage / performance | **Project + installed packages** | Package models (e.g., dbt_artifacts) that run on your warehouse are included |
| Expensive queries | **Project models** | Queries attributed to dbt node_ids in the monitored project |

Graph-dependent recommendations require the dbt project graph. Warehouse and expensive query signals use Snowflake query_history, which provides account-wide visibility scoped to warehouses the project uses.

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

## Package Models Are Disabled by Default

This package is **opt-in** for persistent model builds. After installation:

- **Macros are immediately available** via `dbt run-operation` (no configuration needed)
- **Package models do not run** during your project's normal `dbt run` / `dbt build`

To build package models, explicitly opt in:

**Recommended: dedicated scheduled jobs** (no changes to existing jobs required)
```bash
# All package models
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select package:dbt_cost_optimization_package

# Or select by optimization domain
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:warehouse
```

**Alternative: enable at the project level** (models build on every run)
```yaml
# In a vars.yml file in your project root
vars:
  dbt_cost_optimization_enabled: true
```
Note: if enabled at the project level, you will need to explicitly exclude package models from general runs where they are not desired. Overriding package vars in `dbt_project.yml` `vars:` is not supported — use a `vars.yml` file or CLI instead.

This avoids unexpectedly querying large `ACCOUNT_USAGE` views on every regular dbt build.

### Suggested cadences for scheduled jobs

| Domain | Selector | Suggested Cadence |
|--------|----------|-------------------|
| Warehouse (sizing, spillage, expensive queries) | `+tag:warehouse` | Weekly |
| AI / Cortex spend | `+tag:ai_spend` | Weekly |
| Materialization (view→table, table→incremental) | `+tag:materialization` | Monthly |
| Clustering candidates | `+tag:clustering` | Monthly |

Example dedicated job:
```bash
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:warehouse
```

---

## Getting Started

After installation, see your platform's documentation for:

- Required permissions and grants
- Package variables and configuration
- Available optimization paths
- Quick start commands

**Snowflake users:** Start with [docs/snowflake/index.md](docs/snowflake/index.md)

---

## Key Configuration Variables

Override these in your project's `vars.yml` or via CLI `--vars`:

### Required

| Variable | Default | Purpose |
|----------|---------|---------|
| `dbt_cost_optimization_enabled` | `false` | Must be `true` to build package models |
| `credit_rate_usd` | `2` | Your Snowflake credit rate (for cost estimation) |

### Scope

| Variable | Default | Purpose |
|----------|---------|---------|
| `dbt_monitored_projects` | `[]` (current project) | List of project names to monitor; `['*']` for all |
| `snowflake_enterprise_edition` | `true` | Set `false` for Standard edition (disables ACCESS_HISTORY features) |

### Thresholds (tune to your environment)

| Variable | Default | Purpose |
|----------|---------|---------|
| `expensive_query_high_cost_threshold` | `500` | Annual cost (USD) above which a query is flagged as high-cost |
| `spillage_aggregate_threshold_gb` | `100` | Total GB spilled across models before recommending warehouse scale-up |
| `clustering_candidates_min_size_gb` | `100` | Minimum table size for clustering analysis |
| `incremental_candidates_min_build_time_sec` | `300` | Minimum build time to consider incremental conversion |

For the full variable reference, see [dbt_project.yml](dbt_project.yml) vars section.

## Repository Structure

```
models/
  shared/          Platform-agnostic models (dbt graph introspection)
  snowflake/
    staging/       Snowflake ACCOUNT_USAGE source staging models
    intermediate/  Transforms, aggregations, and cross-environment discovery
    marts/
      clustering/       Table clustering candidate recommendations
      materialization/  View-to-table and incremental strategy recommendations
      warehouse/        Sizing, spillage, and expensive query recommendations
      ai/              AI/Cortex spend and token efficiency recommendations
      gold/            Dashboard-ready views (cross-domain, deduplicated by model)

macros/
  snowflake/       Snowflake optimization macros and utilities

docs/
  snowflake/       Snowflake setup guide and domain documentation
  redshift/        Redshift documentation
```

## Contributing

Each platform has its own model subtree, source definitions, and documentation. When adding support for a new platform:

1. Create `models/<platform>/staging/`, `intermediate/`, and `marts/` directories
2. Add a `_<platform>_sources.yml` in the staging folder
3. Add platform documentation in `docs/<platform>/`
4. Gate models with `+enabled: "{{ target.type == '<platform>' and var('dbt_cost_optimization_enabled', false) }}"` in `dbt_project.yml`
