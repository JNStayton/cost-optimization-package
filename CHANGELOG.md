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
- `dbt_excluded_schemas` and `dbt_excluded_targets` leave dev deployments out of recommendations. Nothing is excluded by default.
- Clustering key evidence counts a column's filters only from queries that scan the table itself, matching column names whole, so filter shares (and savings) can't exceed their totals and short column names don't pick up longer ones' filters.
- View chains recommend one view per chain. Every view in a chain is credited with the builds of every table downstream of it, priced at its measured recompute cost: a post-hook probe (`select hash_agg(*)`, result cache off, up to `table_materialization_view_probe_limit` views per run) with the average read duration as the fallback. The view with the highest net savings is recommended; the others are `monitor` alternatives naming it, so savings aren't counted twice. New columns: `chain_role`, `chosen_view_for_chain`, `recompute_cost_s`, `recompute_cost_source`. Ephemerals are never candidates.
- View chain evidence on the tables at the end of chains (`int_snowflake__table_upstream_views`). Cross-domain insights gain a `view_chain` signal (it counts toward the two-signal minimum) and `upstream_view_chain`, `upstream_view_count` and `chain_recommended_view` columns. With spillage or an expensive query, the recommended action keeps the model's own action and adds "additionally, materialize `<view>` as a table". The warehouse spillage groups list `affected_models_in_view_chains` with a `view_chain_note`, and the spillage fact and top spillage view show each table's `upstream_view_chain`. The dbt-charts dashboard shows the chain and the note.
- Job-level spillage (`int_snowflake__dbt_job_spillage`): when a few models take most of a dbt job's build time by spilling, `spillage_route_models` recommends routing them to a larger warehouse (a `snowflake_warehouse` config, with candidate warehouses one size up); when spill is widespread, `spillage_job_scale_up` recommends sizing up the job's warehouse, or a dedicated one if other jobs share it. Thresholds: `spillage_job_min_time_share_pct` (25), `spillage_job_actionable_time_share_pct` (75), `spillage_job_routing_max_model_share_pct` (25). Jobs come from dbt platform job ids, or `invocation_id` in the query comment for other runs.
- Spillage effects are measured. Scale-ups (savings null) show `estimated_annual_hours_saved` and `estimated_annual_cost_change_usd` from the spilling runtime and the scaling efficiency of the step up (`warehouse_scale_up_efficiency`, from the Snowflake Summit benchmark), and the reason text states the trade. SQL refactors are priced from the time spilling operators were blocked on disk, read from `GET_QUERY_OPERATOR_STATS` by a new post-hook (`extract_spill_evidence`, into `int_snowflake__query_spill_evidence`) and scaled from the sample to all spilling queries.
- Spillage cost is measured: the runtime of each table's spilling queries (`spilling_execution_s`, attributed through `ACCESS_HISTORY`, which also picks the table's warehouse) at the warehouse's list rate. The aggregate warehouse recommendation uses all of the warehouse's spilling queries. Spillage savings are null rather than estimated from invented per-GB constants, so the savings floor no longer demotes spillage scale-ups.
- Spillage recommendations reach the gold layer by tier (`recommendation_key`), not by matching recommendation text. Heavy local spill on X-Large and larger warehouses is a separate `spillage_sql_refactor` signal (effort `sql_refactor`), and moderate spill is `monitor`. The priority hierarchy is defined once, so a signal's rank is the same in every gold view.
- Environment counts are per deployment (physical table): a table built under several target names counts once, and recommendations aren't repeated for it.
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
