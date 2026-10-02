# Testing guide

How this package is tested, how to run the tests, and what the results should look like.

**Scope today:** Snowflake, on dbt v2. The Databricks, BigQuery, and Redshift models are in beta and have their own data tests. Their unit tests, and dbt v1 support, are planned.

---

## 1. What each kind of test proves

| Test type | Count | What it proves | Needs real account data? | Where it lives |
|---|---|---|---|---|
| **Data tests** | 56 on Snowflake marts, plus 3 on shared models and 1 savings check on the recommendation backlog (warns) | The **real output** has no null keys, and category columns only contain expected values | **Yes.** Run after building the models in a real project. | `data_tests:` blocks in `models/snowflake/marts/**/_*.yml` and `models/shared/_shared.yml` |
| **Unit tests** | 21 | The **transformation logic** is correct: for given input rows, a model returns exactly the expected output rows | **No.** Inputs are supplied in the test. A warehouse connection is still required. Off by default; see §3. | `models/snowflake/marts/gold/_gold__unit_tests.yml`, `models/snowflake/marts/warehouse/_warehouse__unit_tests.yml`, `models/snowflake/intermediate/_snowflake_intermediate__unit_tests.yml` |
| **Macro tests** | 5 | Macros that return values (scores, arrays, generated config text, the scaling-efficiency curve) return exactly what's expected | **No.** A warehouse connection is still required. Off by default; see §3. | `tests/snowflake/macros/` |
| **Integration tests (Snowflake)** | 19 assertions, on Standard and Enterprise edition | The **whole pipeline**, hooks included, turns known account activity into exactly the expected recommendations, savings, and gold-view rows | **No.** Fixture tables stand in for `ACCOUNT_USAGE`. Some fixtures run real queries in your account (see §3). | `integration_tests/snowflake/` |
| **Smoke test (Snowflake)** | every package model and data test, both editions | Every model compiles and runs against the **real** `ACCOUNT_USAGE` views' columns and types, on a full refresh and an incremental run | Reads the real views' structure only (`--empty`) | Commands in §3 |

What the unit tests cover:

- **All 11 Snowflake gold views:** filtering, deduplication, ranking, domain and action mapping, cost aggregation, and credit attribution.
- **`scope_filter`**, the project-scoping logic behind every gold view. There's one test for each combination of `dbt_monitored_projects` (`[]`, a list, `['*']`) and `include_full_platform_insights`.
- **Regression tests** for bugs found and fixed while writing the tests. Each one fails against the old SQL:
  - `vw_snowflake__warehouse_optimizations` labeled mixed actionable/monitor groups `monitor`.
  - `vw_snowflake__top_queried_models` returned no rows with `dbt_monitored_projects: ['*']`.
  - `vw_snowflake__top_spillage_models` duplicated a model that had several spillage signals.
  - `vw_snowflake__user_level_cost_attribution` double-counted queries that read several models, and applied one warehouse rate to all of a user's time.

Every unit test and macro test has been checked to **fail** when the logic it covers is broken, not just to pass. So has every integration assertion: each fix it covers was reverted (a "mutation check") and the assertion failed.

---

## 2. Before you start

- **dbt v2.** The package is tested on dbt v2. dbt v1 isn't supported yet.
- **A Snowflake role that can read `SNOWFLAKE.ACCOUNT_USAGE`** (`IMPORTED PRIVILEGES` on the `SNOWFLAKE` database), plus a warehouse.
- **A dedicated test schema.** Setting up unit tests builds tables (mostly empty) in it. Because the package sets `+schema: dbt_cost_optimization`, models land in `<your target schema>_dbt_cost_optimization`.
- **A dbt profile target** for this repo's profile, `dbt_cost_optimization`, in `~/.dbt/profiles.yml`. Key-pair authentication works well for repeated test runs:

  ```yaml
  # ~/.dbt/profiles.yml (outside the repo). Placeholder values only.
  dbt_cost_optimization:
    target: test
    outputs:
      test:
        type: snowflake
        account: <org>-<account>
        user: <username>
        role: <role with ACCOUNT_USAGE access>
        warehouse: <warehouse>
        database: <database>
        schema: <test schema>
        private_key_path: <path to your .p8 key>
        threads: 8
  ```

> **Never commit credentials.** Keep profiles and keys outside the repo. `target/` and `logs/` are gitignored because compiled SQL and logs contain database and schema names.

Every command below passes `--vars '{dbt_cost_optimization_enabled: true}'`, because the package's models are disabled by default. Add `--target <your target>` if your test target isn't the profile's default.

---

## 3. Running the tests

### Data tests: in a project that installs the package

Data tests check real output, so run them where the package analyzes real activity: in a dbt project that installs this package and has models with query history.

```bash
dbt build --select package:dbt_cost_optimization --vars '{dbt_cost_optimization_enabled: true}'
```

To run only the tests against models you've already built:

```bash
dbt test --select package:dbt_cost_optimization,test_type:data --vars '{dbt_cost_optimization_enabled: true}'
```

### Unit and macro tests: from this repo

Unit tests read the **column types** of each tested model's parents from the warehouse, so the parents have to exist first. They can be empty. This is a one-time setup per test schema, and you only need to repeat it when a parent model's columns change.

```bash
# 1. Build everything the gold views depend on as empty tables, except the two models
#    below and anything downstream of them.
dbt run --select +tag:gold \
  --exclude int_snowflake__warehouse_config+ fct_snowflake__table_clustering_candidates+ \
  --empty --vars '{dbt_cost_optimization_enabled: true}'

# 2. Build those two models normally. Their inputs are empty, so this is quick.
dbt run --select int_snowflake__warehouse_config fct_snowflake__table_clustering_candidates \
  --vars '{dbt_cost_optimization_enabled: true}'

# 3. Build everything else as empty tables.
dbt run --select +tag:gold \
  --exclude int_snowflake__warehouse_config fct_snowflake__table_clustering_candidates \
  --empty --vars '{dbt_cost_optimization_enabled: true}'
```

> **Why two models are built normally:** `--empty` rewrites every `ref()` as `(select * from … where false limit 0)`. That works in a model's `select`, but not in post-hooks that write to a table, which these two models have (`refresh_warehouse_config`, `extract_operator_evidence`, `refresh_column_cardinality`). Building them normally on top of empty inputs avoids the problem.

Then run the tests:

```bash
dbt test --select test_type:unit \
  --vars '{dbt_cost_optimization_enabled: true, dbt_cost_optimization_run_package_tests: true}'
dbt test --select tag:macro_tests \
  --vars '{dbt_cost_optimization_enabled: true, dbt_cost_optimization_run_package_tests: true}'
```

Macro tests don't read any tables, so they don't need the setup step.

### Integration tests (Snowflake): from this repo

`integration_tests/snowflake/` is a dbt project that installs this package from the repo and points its `snowflake_usage` source at fixture tables instead of `SNOWFLAKE.ACCOUNT_USAGE` (`vars.yml`). The fixtures describe known activity: demo models with query history, builds, warehouse metering and events, spillage, dbt platform jobs, and access history. The `assert_*` tests in `tests/` check what the package recommends from it.

Run it from `integration_tests/snowflake/`, with the same profile target as the unit tests. Each edition builds into its own schema, `<target schema>_integration` (Standard) or `<target schema>_integration_enterprise`:

```bash
cd integration_tests/snowflake
dbt deps

# 1. Fixtures and demo models (into <target schema>_fixtures and <target schema>)
dbt build --select "+tag:fixtures models/demo"

# 2. The package and the assertions, Standard edition: full refresh, then incremental
SEL="+tag:gold +fct_snowflake__clustering_key_candidates +fct_snowflake__warehouse_performance_recommendations"
dbt build --select $SEL --exclude tag:fixtures --full-refresh
dbt build --select $SEL --exclude tag:fixtures

# 3. The same on Enterprise edition
EV='{snowflake_enterprise_edition: true, package_test_schema: integration_enterprise}'
dbt build --select $SEL --exclude tag:fixtures --full-refresh --vars "$EV"
dbt build --select $SEL --exclude tag:fixtures --vars "$EV"
```

What it needs from your account:

- **An X-Small warehouse** as the target's warehouse. One fixture runs a query sized to spill on X-Small, for the spillage evidence test.
- **`SNOWFLAKE_SAMPLE_DATA`**, which Snowflake accounts have by default. Another fixture scans it as a query that does disk I/O but doesn't spill.
- **A few cents of credits per run.** The fixtures run real queries: clustering reads, the spill and control queries, and the view-chain demo views the probe measures.

> **After rebuilding the fixtures, run with `--full-refresh` first.** Fixture timestamps are relative to now, and the package's incremental models would otherwise keep rows from the previous fixtures.

### Smoke test (Snowflake): against the real `ACCOUNT_USAGE` views

The integration fixtures have only the columns the package reads. The smoke test builds every package model against the real `SNOWFLAKE.ACCOUNT_USAGE` views with `--empty`, which catches a renamed column or a changed type. It uses the same three-step order as the unit test setup, and leaves out the integration project's own assertions, which need fixture data:

```bash
cd integration_tests/snowflake
V='{snowflake_usage_database: SNOWFLAKE, snowflake_usage_schema: ACCOUNT_USAGE, package_test_schema: smoke_standard, snowflake_enterprise_edition: false}'
dbt build --select package:dbt_cost_optimization \
  --exclude int_snowflake__warehouse_config+ fct_snowflake__table_clustering_candidates+ test_type:singular \
  --empty --full-refresh --vars "$V"
dbt build --select int_snowflake__warehouse_config fct_snowflake__table_clustering_candidates \
  --exclude test_type:singular --full-refresh --vars "$V"
dbt build --select package:dbt_cost_optimization \
  --exclude int_snowflake__warehouse_config fct_snowflake__table_clustering_candidates test_type:singular \
  --empty --full-refresh --vars "$V"
```

Run it again without `--full-refresh` for the incremental path, then repeat both with `package_test_schema: smoke_enterprise, snowflake_enterprise_edition: true`.

> **Why the extra var:** unit and macro tests check the package itself, so they're off by default. Otherwise they'd run in every `dbt build` of a project that installs the package. `dbt_cost_optimization_run_package_tests: true` turns them on.

---

## 4. Expected results

| Command | Expected summary |
|---|---|
| Unit tests | `21 total \| 21 success` |
| Macro tests | `5 total \| 5 success` |
| Integration tests, Standard edition (each run) | `140 total \| 140 success` (models, data tests and assertions) |
| Integration tests, Enterprise edition (each run) | `146 total \| 146 success` |
| Smoke test (each step) | all success, no errors |
| Data tests, in an installing project on Snowflake | all pass: 56 Snowflake mart tests, plus 3 on shared models. The savings check (`assert_snowflake__savings_do_not_exceed_cost`) passes, or warns if a cost formula is off |

The counts grow as tests are added, so treat the run summary as the source of truth.

A failing unit test prints a row-by-row diff, with `expected -> actual` for each value that differs, and `∅` for a missing or extra row. For example:

```
| PRIORITY_RANK | SIGNAL_ID                 | RELATED_SIGNALS_COUNT |
| 2.0           | add_clustering_key_strong | 1.0 -> 2.0            |
```

---

## 5. Writing new tests

- **Put unit tests next to the models they test**, in a `_<folder>__unit_tests.yml` file. Give each one a `description:` that lists its cases, so reviewers can see the coverage without reading the SQL.
- **Give every row the same columns** in `expect:` rows. A column left out of some rows is compared as null in those rows.
- **Give every parent an input.** Use `rows: []` for parents a case doesn't need.
- **Keep time windows stable.** For rows inside a "last N days" window, use dates far in the future (`2099-01-01`). For rows that should fall outside it, use dates far in the past (`2000-01-01`). The tests then don't go stale.
- **Use `overrides: vars:`** to test var-driven behavior, such as the `scope_filter` settings.
- **Prove the test can fail.** Change one expected value, or break the logic the test covers, and confirm it fails with a clear diff. Then restore it.
- **Integration fixtures:**
  - **Type each fixture from the real view.** Start it with `select <columns> from snowflake.account_usage.<view> where false union all ...`, so its columns take the real types and the build fails fast if Snowflake renames one.
  - **Use relative timestamps** (`dateadd(..., current_timestamp())`), so rows always fall inside the package's lookback windows.
  - **Use real query IDs where a hook reads them.** `GET_QUERY_OPERATOR_STATS` rejects IDs that don't exist, so the fixture runs the query with `run_query` and records `last_query_id()`. Add a `-- depends_on: {{ ref(...) }}` hint for any `ref()` used only inside `{% if execute %}`.
  - **Keep slices from changing each other.** Builds that one slice needs, but that shouldn't count as dbt sessions, run from session 1 (no dbt session), so they don't change the user attribution or expensive-query results.
  - **Make assertions edition-aware** with `{% if var('snowflake_enterprise_edition', true) %}` wherever Enterprise should differ. Everywhere else, both editions must give the same result.
  - **Where a value comes from a real measurement** (the view probe, operator stats), assert it's consistent with the measurement rather than a fixed number.
- **Put macro tests in `tests/snowflake/macros/`.** They're enabled and tagged by the `data_tests: dbt_cost_optimization: snowflake: macros:` block in `dbt_project.yml`. Keep that config scoped to `macros:`: config under `data_tests:` also applies to generic tests defined in the models' YAML when the paths overlap.

---

## 6. Not covered by automated tests yet

| Area | How it's checked today |
|---|---|
| **Post-hooks against real `SHOW WAREHOUSES` settings** | The integration tests run every hook, but fixture warehouses don't exist in the account, so the multi-cluster settings come from a simulated `SHOW WAREHOUSES` hook (`integration_tests/snowflake/macros/simulate_show_warehouses.sql`). Check the real path manually (see the checklist below). |
| **The 7 `dbt run-operation` commands** | Manual: they query `SNOWFLAKE.ACCOUNT_USAGE` directly, so test inputs can't reach them (see the checklist below). |
| **Databricks, BigQuery, Redshift** | Data tests only, for now |
| **dbt v1** | Not supported yet |

Planned: CI that runs `dbt parse` on every platform.

### Manual checklist: hooks

Build the package in a real project, then check:

- [ ] `fct_snowflake__incremental_config_recommendations`: the log shows `probe_unique_key_candidates: starting exact uniqueness probe`. Where there are merge candidates, `likely_unique_key` is filled in.
- [ ] `fct_snowflake__table_clustering_candidates`: the log shows `extract_operator_evidence:` and `refresh_column_cardinality:` messages. `int_snowflake__query_operator_evidence` and `int_snowflake__column_cardinality` have fresh rows.
- [ ] `int_snowflake__warehouse_config`: the log shows `refresh_warehouse_config: merged config for all warehouses`, and `auto_suspend`, `scaling_policy`, and the cluster counts are filled in. A warehouse with `max_cluster_count > 1` has `is_multicluster` true.
- [ ] `int_snowflake__view_probe`: the log shows `probe_view_recompute:` messages, and views in view chains have rows with `probe_status = 'ok'`.
- [ ] `fct_snowflake__warehouse_performance_recommendations` (Enterprise): the log shows a per-table `extract_spill_evidence: ... analyzed N of M sampled spilling queries` line, and `int_snowflake__query_spill_evidence` has rows.

### Manual checklist: `run-operation` commands

Run each command with its defaults, then with at least one argument override. Confirm that the `Criteria:` line in the log reflects the override. Do this on both a Snowflake **Enterprise** and a **Standard** edition account.

```bash
dbt run-operation find_table_clustering_candidates --args '{lookback_days: 14}'
dbt run-operation suggest_clustering_keys --args '{model_name: <a model>, include_boolean_cols: true}'
dbt run-operation find_table_materialization_candidates --args '{lookback_days: 7, min_query_count: 30}'
dbt run-operation find_incremental_materialization_candidates --args '{min_table_size_gb: 50}'
dbt run-operation find_warehouse_sizing_recommendations --args '{lookback_days: 14}'
dbt run-operation find_spillage_candidates --args '{lookback_days: 14}'
dbt run-operation find_expensive_dbt_queries --args '{top_n: 10}'
```

On a platform without a Snowflake implementation, each command should fail with `… is not yet implemented for '<platform>'`.
