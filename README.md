# dbt Cost Optimization Package

A dbt package that analyzes your data platform's compute, storage, and query patterns to identify optimization opportunities and produce actionable recommendations.

## Supported platforms

| Platform | Status | Documentation |
|----------|--------|---------------|
| **Snowflake** | GA | [docs/snowflake/](docs/snowflake/index.md) |
| **Databricks** | Beta [WIP] | [docs/databricks/](docs/databricks/) |
| **BigQuery** | Beta [WIP] | [docs/bigquery/](docs/bigquery/clustering_key_candidates.md) |
| **Redshift** | Beta [WIP] | [docs/redshift/](docs/redshift/) |

## What you'll get

### Quick-use commands

On-demand optimization checks you can run with `dbt run-operation`, with no models to build first. Currently available for Snowflake:

| Command | What it finds |
|---------|---------------|
| `find_table_clustering_candidates` | Tables that would benefit from clustering |
| `suggest_clustering_keys` | The best clustering key columns for a specific model |
| `find_table_materialization_candidates` | Views queried often enough to materialize as tables |
| `find_incremental_materialization_candidates` | Large, slow-building tables suited to incremental models |
| `find_warehouse_sizing_recommendations` | Warehouse sizing changes (scale up or down, multi-cluster, Gen2) |
| `find_spillage_candidates` | Models whose builds spill to local or remote storage |
| `find_expensive_dbt_queries` | The most expensive recurring dbt queries by projected annual cost |

For example: `dbt run-operation find_table_clustering_candidates --args '{lookback_days: 14}'`. See [docs/snowflake/macros.md](docs/snowflake/macros.md) for every command's arguments.

### Snowflake

The package produces **gold-layer views**: dashboard-ready outputs that surface ranked, prioritized recommendations.

| View | Audience | What it shows |
|------|----------|--------------|
| `vw_snowflake__top_recommendations` | Executives / leads | Top-priority recommendations across all domains, deduped per entity |
| `vw_snowflake__dbt_model_optimizations` | dbt engineers | Actionable model changes: clustering keys, materialization, incremental configs |
| `vw_snowflake__warehouse_optimizations` | Snowflake admins | Warehouse config changes: auto-suspend, scaling, sizing — with linked model context |
| `vw_snowflake__optimization_backlog` | Sprint planning / agents | Full inventory of all signals (all priority tiers) for ticket creation |
| `vw_snowflake__top_expensive_queries` | Cost owners | Top 10 expensive queries enriched with root-cause co-signals |
| `vw_snowflake__top_spillage_models` | Performance engineers | Models causing the most memory spillage, with dbt platform run traceability |
| `vw_snowflake__top_queried_models` | Platform engineers | Most-queried models (downstream consumption pressure) |
| `vw_snowflake__cross_domain_insights` | Architecture leads | Multi-signal correlation (why issues co-occur on the same model) |
| `vw_snowflake__cost_savings_summary` | Dashboards | KPI tiles: total opportunity per domain |
| `vw_snowflake__user_level_cost_attribution` | Cost owners | User-level cost attribution for chargeback |
| `vw_snowflake__ai_optimizations` | AI/ML teams | Cortex model cost, token efficiency, agent spend |

Each recommendation includes a `priority_tier`, a per-entity relative ordering:
- **P1** = do this first (highest in the optimization hierarchy for this model/warehouse)
- **P2** = do this second (after P1 is applied)
- **P3+** = deferred (waiting for higher-priority fixes to resolve the symptom)

Priority cascades naturally: when you apply a P1 fix (e.g., add a clustering key) and rebuild, the signal disappears and P2 promotes to P1 automatically.

### Databricks, BigQuery, and Redshift (Beta)

These platforms produce recommendations at the fact-model layer. Dashboard views and charts like Snowflake's are coming soon.

| Platform | Recommendations |
|----------|-----------------|
| **Databricks** | Liquid clustering, OPTIMIZE, table and incremental materialization, snapshot optimization, and model run trends, plus a per-model rollup (`vw_databricks__recommendations_by_model`) |
| **BigQuery** | Table clustering candidates and clustering key recommendations |
| **Redshift** | Sort key and distribution key recommendations, table and incremental materialization, incremental config, and VACUUM and ANALYZE candidates |

See each platform's docs for model details and configuration.

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

Installation from the dbt package hub (`package:` / `version:`) is coming soon.

## Getting started

After installation, see your platform's documentation for required permissions, configuration, and quick start commands:

- **Snowflake:** [docs/snowflake/index.md](docs/snowflake/index.md)
- **Databricks:** [docs/databricks/](docs/databricks/)
- **BigQuery:** [docs/bigquery/](docs/bigquery/clustering_key_candidates.md)
- **Redshift:** [docs/redshift/](docs/redshift/)

## Configuration

The package is **opt-in**. Package models only build when `dbt_cost_optimization_enabled` is `true`.

To customize the package's behavior, create a `vars.yml` file in your project root:

```yaml
# vars.yml
vars:
  dbt_cost_optimization_enabled: true
```

Or pass vars on the command line with `--vars '{var_name: value}'`. Overriding package vars in your own `dbt_project.yml` `vars:` section is not supported.

Every other var has a default. For the full list, see your platform's documentation or the `vars:` section of [dbt_project.yml](dbt_project.yml), which groups them into shared and per-platform sections.

## How it works

### Package models are disabled by default

After installation:

- **Snowflake optimization commands are immediately available** via `dbt run-operation` (no configuration needed)
- **Package models do not run** during your project's normal `dbt run` / `dbt build`

To build package models, explicitly opt in.

**Recommended: dedicated scheduled jobs** (no changes to existing jobs required)
```bash
# All package models
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select package:dbt_cost_optimization_package

# Or select by optimization domain
dbt build --vars '{dbt_cost_optimization_enabled: true}' --select +tag:clustering
```

**Alternative: enable in your `vars.yml`** (models build on every run)

If you set `dbt_cost_optimization_enabled: true` in your `vars.yml`, you will need to explicitly exclude package models from general runs where they are not desired.

Either way, this avoids unexpectedly querying large platform system tables (such as Snowflake's `ACCOUNT_USAGE` views) on every regular dbt build. Only the models for your data platform are enabled.

### Suggested cadences for scheduled jobs

| Domain | Selector | Platforms | Suggested cadence |
|--------|----------|-----------|-------------------|
| Warehouse (sizing, spillage, expensive queries) | `+tag:warehouse` | Snowflake | Weekly |
| AI / Cortex spend | `+tag:ai_spend` | Snowflake | Weekly |
| Materialization (view→table, table→incremental) | `+tag:materialization` | Snowflake, Databricks, Redshift | Monthly |
| Clustering candidates | `+tag:clustering` | All | Monthly |
| Dashboard views | `+tag:gold` | Snowflake, Databricks | After the domains above |

Every package mart also has the `dbt_cost_optimization` tag.

### Scope (Snowflake)

| What | Scope | Notes |
|------|-------|-------|
| Model-level recommendations (clustering, materialization, incremental) | **This project only** | Requires dbt graph context (model configs, lineage) |
| Warehouse-level recommendations (config changes, spillage, idle credits) | **Warehouses used by this project** | Surfaces for any warehouse that runs project models |
| Spillage / performance | **Project + installed packages** | Package models (e.g., dbt_artifacts) that run on your warehouse are included |
| Expensive queries | **Project models** | Queries attributed to dbt node_ids in the monitored project |

Graph-dependent recommendations require the dbt project graph. Warehouse and expensive query signals use Snowflake query_history, which provides account-wide visibility scoped to warehouses the project uses.

To leave dev deployments out of the Snowflake recommendations, set `dbt_excluded_schemas` (schema patterns, e.g. `['DBT_%']`) or `dbt_excluded_targets` (e.g. `['dev']`) in your `vars.yml`. By default nothing is excluded.

On Databricks, BigQuery, and Redshift, recommendations are scoped to your project's dbt models by default. See each platform's docs for the scope settings.

## Visualize your results

For Snowflake, once you've built the gold-layer models, see
[integrations/dbt_charts/README.md](integrations/dbt_charts/README.md) to
launch a [dbt-charts](https://github.com/dbt-labs/dbt-charts) dashboard
over them.

## Repository structure

```
models/
  shared/              Platform-agnostic models (dbt graph introspection)
  snowflake/
    staging/           Source staging models
    intermediate/      Transforms, aggregations, and cross-environment discovery
    marts/
      clustering/      Table clustering candidate recommendations
      materialization/ View-to-table and incremental strategy recommendations
      warehouse/       Sizing, spillage, and expensive query recommendations
      ai/              AI/Cortex spend and token efficiency recommendations
      gold/            Dashboard-ready views (cross-domain, deduplicated by model)
  databricks/          staging/, intermediate/, marts/
  bigquery/            staging/, intermediate/, marts/
  redshift/            staging/, intermediate/, marts/

macros/
  _macros.yml          Every macro's arguments and which platforms implement it
  optimizations/       Commands you run with dbt run-operation (and their helpers)
  utils/               Internal utilities used by models and hooks
  platforms/           Each platform's implementation of the macros above

docs/
  snowflake/           Setup guide, permissions, configuration, and reference docs
  databricks/          Model docs
  bigquery/            Model docs
  redshift/            Model docs and platform notes
```

Macros use `adapter.dispatch`, so each command keeps the same name across platforms and runs the right implementation for your data platform.

## Contributing

We welcome bug reports and feature requests. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to open an issue. We aren't accepting outside pull requests yet.

## Support

This package is provided as-is, without SLAs, and is maintained on a best-effort basis. To report a bug or request a feature, open a GitHub issue. We read every issue, but we can't guarantee response times.

## Security

Please don't report security vulnerabilities in a public issue. Use the **Security** tab of this repository to report them privately.

## License

This package is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
