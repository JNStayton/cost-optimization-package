# dbt Cost Optimization Package

A dbt package that analyzes your data platform's compute, storage, and query patterns to identify optimization opportunities and produce actionable recommendations.

## Supported Platforms

| Platform | Status | Documentation |
|----------|--------|---------------|
| **Snowflake** | Active | [docs/snowflake/index.md](docs/snowflake/index.md) |
| **Redshift** | In development | [docs/redshift/](docs/redshift/) |
| **BigQuery** | In development | — |
| **Databricks** | Planned | — |

## What It Does

This package builds dbt models and provides macros that:

- Identify tables that would benefit from clustering, materialization changes, or incremental strategies
- Detect warehouse sizing opportunities (scale up, scale down, Gen2, multi-cluster)
- Surface expensive queries and spillage patterns
- Monitor AI/Cortex spend and token efficiency (Snowflake)
- Produce ranked, explainable recommendations with confidence signals

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
