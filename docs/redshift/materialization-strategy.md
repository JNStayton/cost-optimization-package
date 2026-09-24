# Redshift materialization strategy

This document defines how each model layer in the Redshift implementation of the cost-optimization package is materialized, and why.
It exists because the package has two distinct classes of correctness constraints that pull in opposite directions.

1. **Data-loss risk from retention windows.**
Several Redshift system views are time-bounded — Amazon ages out their rows on a fixed schedule.
If they're wrapped as plain views in staging, every dbt run only sees Redshift's current retention window, and anything older that has aged out is permanently lost.
2. **Redshift planner failures on complex view chains.**
Deep chains of views that mix leader-only sources (SVV / pg_catalog) with compute-side sources (SYS) can trip Redshift's query optimizer with `Assert: Hash table for subplan does not exist (SQLSTATE XX000)` and similar internal errors.

These two constraints require opposite materialization choices in different places.
This document is the policy that reconciles them, followed by a reference list of Redshift-specific platform behaviors this package works around.

---

## Source-by-source classification

Every Redshift system source the package reads is classified as either **current-state** (a real-time view of what exists right now) or **time-bounded log** (a rolling window of historical events that Amazon ages out).
The classification is sourced from [AWS's official types-of-views documentation](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_system-tables.html#c_types-of-system-tables-and-views).

| Source | View family | Behavior | Retention per AWS docs |
|---|---|---|---|
| `pg_class` | pg_catalog | Current state | N/A — current state |
| `pg_namespace` | pg_catalog | Current state | N/A — current state |
| `pg_depend` | pg_catalog | Current state | N/A — current state |
| `pg_rewrite` | pg_catalog | Current state | N/A — current state |
| `svv_tables` | SVV | Current state | N/A — current state |
| `svv_table_info` | SVV | Current state | N/A — current state |
| `svv_columns` | SVV | Current state | N/A — current state |
| `svv_alter_table_recommendations` | SVV | Current state (advisor recs disappear once applied) | N/A — current state |
| `sys_query_history` | SYS | Time-bounded log | Not explicitly stated in per-view docs. STL (the legacy equivalent) is documented as 7 days. |
| `sys_query_detail` | SYS | Time-bounded log | Same — not explicitly stated per-view, but bounded. |
| `sys_serverless_usage` | SYS | Time-bounded log | Explicitly documented: 7 days ([SYS_SERVERLESS_USAGE](https://docs.aws.amazon.com/redshift/latest/dg/SYS_SERVERLESS_USAGE.html)). |
| `sys_connection_log` | SYS | Time-bounded log | Not explicitly stated per-view. |

AWS doesn't publish a per-view retention number for every SYS view.
The SYS family clearly does age data out — that's its purpose — and the one view that documents retention explicitly (`SYS_SERVERLESS_USAGE`) states 7 days.
Treat all SYS views as bounded with an assumed minimum window of 7 days unless AWS publishes something longer.

---

## Materialization policy

### Staging layer (`models/staging/redshift/`)

| Source type | Materialization | Rationale |
|---|---|---|
| pg_catalog passthrough (`pg_class`, `pg_namespace`, `pg_depend`, `pg_rewrite`) | `view` | Catalogs are tiny and current-state; materializing as a table is unnecessary. |
| SVV current-state (`svv_tables`, `svv_table_info`, `svv_columns`, `svv_alter_table_recommendations`) | `table` | These are leader-only system views. Wrapping them as plain views propagates the leader-only constraint into every downstream join, and a downstream model joining multiple SVV sources with compute-side sources can trip the planner assertion above. Materializing as a table lands the data on compute nodes at the staging boundary. The rows are tiny (one per table or one per column), so the storage cost is trivial. List columns explicitly (not `select *`) to avoid view-resolution surprises. |
| SYS time-bounded (`sys_query_history`, `sys_query_detail`, etc.) | `incremental` | Required for correctness — a plain view loses any rows that fall off Redshift's retention window between runs. Use a `start_time`-based incremental filter to accumulate history across runs. |

### Intermediate layer (`models/intermediate/redshift/`)

| Model shape | Materialization | Rationale |
|---|---|---|
| Recursive CTEs (e.g., `int_redshift__view_chains`, `int_redshift__view_terminal_ancestors`) | `table` | Recursion itself works fine over an already-materialized parent table, but if a downstream consumer performs more than a simple column read against the recursive model's output (e.g. a lateral SUPER array unnest), leaving it as a `view` forces Redshift to re-inline the `WITH RECURSIVE` definition at query time, which is unreliable — it can either error outright or silently produce wrong aggregates. |
| Models that join leader-only catalogs with compute-side data | `table` | Leader-only sources can't be joined to compute-side data inside a view definition. Materializing breaks the leader/compute boundary at this point. |
| Recommendation-output intermediates that are queried with predicates by tests or a mart | `table` | Redshift's optimizer can fail to push filters through a multi-CTE view definition. Materializing the output as a table eliminates the planner-pushdown chain entirely. |
| Pre-materialized building blocks (where a model is too complex for a single CTAS) | `table` | Break the model into smaller intermediates, each materialized as a table, and have the final model be a flat join across them. |
| Anything else (simple passthroughs, joins between compute-side sources only) | `view` | Default. Cheap, no storage cost. |

### Marts layer (`models/marts/redshift/`)

| Model | Materialization | Rationale |
|---|---|---|
| `fct_redshift__warehouse_optimization_recommendations` | `incremental` | Snapshot-per-day recommendation history. Append-only. |

---

## Known Redshift platform quirks

Reference list of Redshift-specific behaviors this package works around. Relevant if you're modifying a model that reads from `sys_query_history`, `sys_query_detail`, or the pg_catalog/SVV sources directly.

- **`CREATE OR REPLACE VIEW` rejects column-type changes.** If a staging model's cast changes between package versions, `CREATE OR REPLACE VIEW` fails with `SQLSTATE 42P16`. Remedy: `DROP VIEW IF EXISTS <schema>.<view> CASCADE;`, then re-run.
- **`oid`-typed columns can't be persisted by CTAS.** Any pg_catalog/SVV column exposing a native `oid` type (e.g. `pg_class.oid`, `svv_table_info.table_id`) must be cast to `bigint` in staging before a `table`-materialized model can select it.
- **Some window function patterns aren't supported.** `COUNT(DISTINCT col) OVER (...)` and similar DISTINCT-aggregate window functions fail with `WINDOW definition is not supported (SQLSTATE XX000)`. Use `row_number()` plus a `GROUP BY` with conditional `MAX/SUM(CASE WHEN rn = ... THEN col END)` instead.
- **A SUPER unnest immediately followed by a join on the unnested value fails.** `invalid join condition for SUPER unnest join (SQLSTATE 0A000)`. Isolate the unnest into its own CTE, extracting the element into a regular typed column, before joining on it.
- **CTAS infers a `varchar` column's width from a single CASE branch, not the widest one.** A column built from a multi-branch `CASE` expression with varying string lengths can hit `value too long for type character varying(N) (SQLSTATE 22001)` once a longer branch fires on real data. Wrap the whole expression in an explicit `cast(... as varchar(N))` sized for the longest realistic branch.
- **`sys_query_history`'s time columns (`elapsed_time`, `queue_time`, `execution_time`, `compile_time`, `planning_time`, `lock_wait_time`) are in microseconds, not milliseconds.** Divide by `1,000,000`, not `1,000`, to get seconds.
- **`sys_query_detail`'s `input_bytes` is always 0 for `scan` steps.** A scan is a leaf node in the execution plan with no upstream step to receive input from. `output_bytes` is the column that reflects what a scan step actually read from storage.
- **A table's `table_id` changes on every rebuild.** dbt's `table` materialization does `CREATE` + `RENAME` + `DROP` each time, assigning a new physical `table_id`. Any mechanism that needs to identify the same *logical* table across multiple historical rebuilds should match by name or by the dbt node_id embedded in the query comment, not by `table_id`.
- **A CTAS's `insert` step reports the temp object's `table_name`, not the final table's.** dbt's table materialization inserts into a `__dbt_tmp`-suffixed temp relation before renaming it into place. Identifying a CTAS's real target by the insert step's `table_name` directly won't match a candidate table's real name; use the dbt node_id embedded in the query comment instead, or strip the `__dbt_tmp` suffix if node_id isn't available (e.g. for non-dbt-managed tables).
- **Late-binding views (`bind=false`) have no `pg_depend`/`pg_rewrite` entries at all.** `bind=false` is a documented dbt-redshift production pattern (required for any view whose lineage touches an external or shared-storage table), so any mechanism that needs to resolve a view's dependencies should use the dbt manifest graph, not Redshift's own catalogs.
- **Redshift has no `microbatch` incremental strategy.** Models that would receive `microbatch` on Snowflake receive `append` instead.

---

## Decision flowchart for new models

When you add a new model in this package, decide its materialization in this order:

1. **Does it consume a SYS time-bounded source as a primary input?**
   → If yes, the staging model for that source must be `incremental`. The intermediate model can be `view` or `table` depending on planner-stability concerns.
2. **Is it a recommendation-output intermediate** (produces rows that a mart or tests query with `WHERE` filters)?
   → `table`.
3. **Does it join leader-only sources (SVV / pg_catalog) with compute-side sources (SYS)?**
   → `table` for the model where the join happens.
4. **Does it use a recursive CTE?**
   → `table` if any downstream consumer does more than a simple column read against it (see the quirks list above); `view` is fine otherwise, but every recursive reference must be to an already-`table` ancestor.
5. **Otherwise:** `view`. Default.

---

## Open questions / not yet resolved

- **Exact retention for `sys_query_history` and `sys_query_detail`.** AWS publishes 7 days for `SYS_SERVERLESS_USAGE` and the legacy STL views, but no explicit number for the query SYS views. Until AWS clarifies, the package assumes ≥7-day retention, and the lookback variables (`table_materialization_lookback_days`, `incremental_candidates_lookback_days`) should be tuned to the customer's observed retention.
