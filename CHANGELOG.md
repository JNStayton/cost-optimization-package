# Changelog

All notable changes to this package are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - Unreleased

First public release: one package with a shared design across Snowflake, Databricks, BigQuery, and Redshift.

### Added

#### Package design (all platforms)
- Platform-first layout: each platform's models live in `models/<platform>/` (staging, intermediate, marts), and cross-platform models live in `models/shared/`.
- Opt-in by default: package models only build when `dbt_cost_optimization_enabled: true`, and only the models for your data platform are enabled.
- Macros behind `adapter.dispatch`: each command and utility keeps one public name and runs the right implementation for your data platform, or raises a clear "not yet implemented" error where one doesn't exist yet. Implementations live in `macros/platforms/<platform>/`, and `macros/_macros.yml` documents every macro's arguments and which platforms implement it.
- Domain tags on marts for scheduled jobs (`+tag:clustering`, `+tag:materialization`, `+tag:warehouse`, `+tag:ai_spend`, `+tag:gold`), plus `dbt_cost_optimization` on every mart.
- Package vars grouped into package-wide, shared, and per-platform sections in `dbt_project.yml`. Override them in a `vars.yml` file in your project root or with `--vars`.
- Shared dbt graph models: `int_dbt__relations` (models) and `int_dbt__snapshots` (snapshots).

#### Snowflake (GA)
- Eleven dashboard-ready gold views, including `vw_snowflake__top_recommendations`, `vw_snowflake__dbt_model_optimizations`, `vw_snowflake__warehouse_optimizations`, `vw_snowflake__optimization_backlog`, and `vw_snowflake__cost_savings_summary`.
- Optimization domains:
  - **Warehouse:** sizing, spillage (aggregate and per-model), idle credits, expensive queries, Gen2, and multi-cluster recommendations
  - **Materialization:** view-to-table candidates and incremental candidates with confidence scoring
  - **Clustering:** pruning-based candidate scoring and clustering key recommendations from query operator stats
  - **AI/Cortex:** model cost, token efficiency, user concentration, and batch opportunities
- Per-entity priority tiers (P1, P2, P3+) that cascade as optimizations are applied.
- Confidence-based incremental recommendations: strategy inferred from data semantics, a 0–100 confidence score with explicit assumptions and blocking signals, and an exact unique key probe.
- Scope filtering with `dbt_monitored_projects`, and dbt platform run and job traceability in the spillage and expensive query views.
- Quick-use `dbt run-operation` commands: `find_table_clustering_candidates`, `suggest_clustering_keys`, `find_table_materialization_candidates`, `find_incremental_materialization_candidates`, `find_warehouse_sizing_recommendations`, `find_spillage_candidates`, and `find_expensive_dbt_queries`.
- Support for Enterprise and Standard editions (`snowflake_enterprise_edition`).
- Cost and savings estimates at Snowflake's published credits-per-hour rate for each model's own warehouse (X-Small when unknown), annualized over each domain's lookback window. User cost attribution uses `ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY` credits where available, with elapsed time × list rate as the fallback, and flags which (`credits_from_attribution`).
- A [dbt-charts](https://github.com/dbt-labs/dbt-charts) dashboard over the gold views, in `integrations/dbt_charts/`.
- A data test that warns when any recommendation's estimated savings exceed its estimated cost, as a check on the cost formulas against your real data.
- Tolerates non-dbt query traffic: query comments and session metadata that aren't valid JSON are treated as non-dbt activity instead of failing the build.
- Clustering operator evidence skips queries on warehouses the package's role can't monitor, instead of failing the build, and logs a per-table coverage summary. Grant `MONITOR` on those warehouses for full coverage (see the Snowflake permissions docs).

#### Databricks (Beta)
- Liquid clustering, OPTIMIZE, table materialization, incremental materialization, and snapshot optimization candidates.
- Model run summary for performance trends.
- Per-model recommendation rollup (`vw_databricks__recommendations_by_model`).

#### BigQuery (Beta)
- Table clustering candidates and clustering key recommendations.
- Optional query-text column attribution (`use_query_text_attribution`).

#### Redshift (Beta)
- Sort key and distribution key recommendations.
- Table materialization, incremental materialization, and incremental config recommendations.
- VACUUM and ANALYZE candidates.

[Unreleased]: https://github.com/dbt-labs/dbt-cost-optimization-package/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/dbt-labs/dbt-cost-optimization-package/releases/tag/v1.0.0
