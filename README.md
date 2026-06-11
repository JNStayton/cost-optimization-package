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
  snowflake/       Snowflake staging, intermediate, and mart models
  redshift/        Redshift staging and intermediate models
  bigquery/        BigQuery staging and intermediate models
  databricks/      Databricks (planned)

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
4. Gate models with `+enabled: "{{ target.type == '<platform>' }}"` in `dbt_project.yml`
