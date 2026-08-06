# Changelog

## v1.0.0 — GA Release (August 2026)

Initial GA release of the dbt Cost Optimization Package for Snowflake.

### Gold Layer Views (10 dashboard-ready outputs)

- `vw_snowflake__top_recommendations` — P1+P2 recommendations across all domains, deduplicated per entity
- `vw_snowflake__dbt_model_optimizations` — Actionable model changes with dbt config templates
- `vw_snowflake__warehouse_optimizations` — Warehouse config changes with ready-to-run DDL
- `vw_snowflake__optimization_backlog` — Full signal inventory for sprint planning and agent intake
- `vw_snowflake__cross_domain_insights` — Multi-signal correlation (why issues co-occur on the same model)
- `vw_snowflake__top_expensive_queries` — Top 10 expensive queries with co-occurring fix signals
- `vw_snowflake__top_spillage_models` — Models causing the most memory spillage, with dbt Cloud traceability
- `vw_snowflake__top_queried_models` — Most-queried models by SELECT consumption
- `vw_snowflake__cost_savings_summary` — KPI tiles: total opportunity per domain
- `vw_snowflake__user_level_cost_attribution` — User-level cost attribution for chargeback

### Priority System

- Per-entity relative priority ordering (not fixed global tiers)
- Hierarchy rank: always-safe config > incremental/materialization > clustering > conditional config > monitor
- Priority cascades automatically as optimizations are applied
- Backlog status sort ensures investigate items never outrank actionable ones

### Confidence-Based Incremental Recommendations

- Strategy inference from data semantics (not table size): merge when key + watermark + no deletes; append when INSERT-only
- Confidence scoring (0-100) with explicit assumptions and blocking signals arrays
- Exact unique key probe: `count(*) = count(distinct key) AND count_if(key IS NULL) = 0`
- Four recommendation statuses: `actionable_review`, `investigate`, `do_not_recommend`
- Phase 1 actionable strategies: merge and append only

### Scope Filtering

- Model-level recommendations: project models only (requires dbt graph context)
- Warehouse-level recommendations: any warehouse the project uses
- Spillage/performance: project + installed packages
- Configurable via `dbt_monitored_projects` variable

### dbt Cloud Traceability

- `dbt_cloud_run_id` and `dbt_cloud_job_id` parsed from query comments through staging/intermediate layers
- Surfaced in spillage and expensive query views for linking back to specific builds

### Optimization Domains

- **Warehouse**: sizing, spillage (aggregate + per-model), idle credits, expensive queries, Gen2, MCW
- **Materialization**: view-to-table candidates, incremental candidates with confidence scoring
- **Clustering**: V3 pruning-based scoring, key recommendations via operator stats analysis
- **AI/Cortex**: model cost, token efficiency, user concentration, batch opportunities

### Package Design

- All models disabled by default (`dbt_cost_optimization_enabled: true` to opt in)
- Macros available immediately after `dbt deps` (no opt-in needed)
- Tag-based domain selectors for scheduled jobs (`+tag:warehouse`, `+tag:materialization`, etc.)
- `int_snowflake__all_recommendations` materialized as TABLE for performance
