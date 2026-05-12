# Incremental configuration recommendations — Snowflake

## Overview

`fct_snowflake__incremental_config_recommendations` identifies dbt `table`-materialized models that are candidates for conversion to `incremental` materialization, detects likely incremental key columns, and recommends a Snowflake-optimized strategy. The output includes a copy-pasteable dbt config template and a validation SQL snippet for confirming key column uniqueness before implementing.

This model depends on `fct_snowflake__incremental_materialization_candidates` (Model 1), which handles candidate identification and redundancy scoring.

---

## Strategy decision matrix

The recommended strategy is derived from three signals: the availability of a timestamp/date filter column, the availability of a unique key candidate, and table scale (row count and size).

| Scenario | Recommended strategy | Rationale |
|---|---|---|
| Unique key + filter column + large scale (>10M rows or >10 GB) | `delete+insert` | Scopes deletes to the filter window; avoids full-target merge scan at large scale |
| Unique key + filter column + moderate scale | `merge` | Standard default; merge cost is acceptable at moderate scale |
| Filter column only, no external DML, high volume | `microbatch` | Processes data in self-healing time batches; best for reliability at scale |
| Filter column only, no external DML | `append` | Simplest and cheapest; suitable when source data is truly append-only |
| Unique key only, no filter column | `merge` | No time boundary available; merge is the only safe option |
| External deletes detected | `delete+insert` (flagged for review) | Window-scoped only — deletes outside the defined window still require a periodic full-refresh |
| No key candidates detected | `append` | Safest default; config template flags key identification as a required next step |

---

## Snowflake strategy performance guide

Snowflake's incremental strategies differ significantly in cost. Prefer strategies that scope work to a bounded window rather than scanning the full target table.

### `append`
Inserts new rows only. No matching against existing rows. The cheapest strategy and the right choice when the source is truly append-only (no late-arriving updates or deletes).

### `microbatch` *(dbt Core 1.9+)*
Processes data in configurable time-based batches. Each batch is a scoped CTAS replacing its time window, which keeps individual operations small. Self-healing: failed batches can be retried independently without a full-refresh. Best for high-volume, append-only pipelines with a reliable event timestamp.

### `insert_overwrite`
Replaces entire Snowflake micro-partitions (or clustering ranges) in the target. Efficient when the table has a well-defined clustering key and data lands in clean partition boundaries. Requires the table to already be clustered; do not use without confirming the clustering setup.

### `delete+insert`
Deletes matching rows from the target scoped to a filter window, then inserts new rows. More efficient than `merge` for large tables because the delete is bounded by the filter predicate rather than scanning the entire target. Requires both a `unique_key` and a filter column. The window logic must account for late-arriving records.

### `merge`
Uses a Snowflake `MERGE` statement to match on `unique_key` and update or insert. The default when `unique_key` is set. At large scale, the match scan reads the full target table, making it the most expensive strategy. Prefer `delete+insert` for tables above ~10M rows or ~10 GB.

**Cost ranking (cheapest → most expensive):** `append` → `microbatch` → `insert_overwrite` → `delete+insert` → `merge`

---

## Key column detection

### Filter column (`suggested_filter_column`)
Identifies the best timestamp or date column to use in the `{% if is_incremental() %} where <col> > (select max(<col>) from {{ this }}) {% endif %}` filter.

Detection order:
1. Restrict to columns with a timestamp or date data type (`TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP`, `DATE`)
2. Rank by column name pattern:
   - `*updated_at*`, `*modified_at*` — highest priority; captures late-arriving record changes
   - `*loaded_at*`, `*ingested_at*`, `*inserted_at*`, `*synced_at*` — ingestion time; tool-agnostic
   - `*created_at*`, `*event_date*`, `*event_time*`, `*event_timestamp*` — creation or event time
   - Any other timestamp/date column — lowest priority

### Unique key (`unique_key_candidates`)
Identifies columns that are plausible unique key candidates based on naming conventions. Returns a ranked list rather than a single assertion — uniqueness must be validated before implementing.

Detection criteria:
- Column name matches `id` (exact), `*_id`, `*_key`, `*_sk` (surrogate key), `surrogate_key`, `primary_key`
- Integer or string data types preferred
- Ranked by name specificity: surrogate/primary key names > exact `id` > `*_id` patterns

**Uniqueness is not verified at query time.** The model provides a `validate_uniqueness_sql` snippet for each top candidate:
```sql
select count(*) = count(distinct <column>) as is_unique from <schema>.<table>
```
Run this before implementing to confirm the column is suitable as a `unique_key`.

---

## Implementing the recommendation

1. Run the validation SQL from `validate_uniqueness_sql` to confirm the unique key candidate
2. Copy the `dbt_config_template` into your model file
3. Fill in the `unique_key` placeholder with the validated column
4. Add a `unique` test to your schema YAML for the chosen key column
5. Run a full-refresh on the first incremental run: `dbt run --full-refresh --select <model>`

---

## Variables

| Variable | Default | Description |
|---|---|---|
| `incremental_candidates_lookback_days` | 60 | Lookback window for build history analysis |
| `incremental_candidates_min_build_time_sec` | 300 | Minimum max build time to trigger on build time alone |
| `incremental_candidates_min_size_gb` | 2 | Minimum table size to trigger on size alone |
| `incremental_candidates_min_compute_waste_score` | 5 | Minimum size × builds/day score to trigger on waste alone |
| `incremental_candidates_min_compute_waste_avg_build_sec` | 30 | Minimum average build time required alongside waste score trigger |
| `incremental_candidates_min_qualified_build_days` | 3 | Minimum CTAS build days required to trust the growth signal |
| `incremental_large_table_row_threshold` | 10000000 | Row count above which `delete+insert` is preferred over `merge` |
| `incremental_large_table_gb_threshold` | 10 | Size in GB above which `delete+insert` is preferred over `merge` |
