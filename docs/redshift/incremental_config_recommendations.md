# Redshift incremental config recommendations

## fct_redshift__incremental_config_recommendations

Model 2 of the incremental materialization recommendation flow. Depends on `fct_redshift__incremental_materialization_candidates` (Model 1).

Excludes tables with `recommendation = 'Low ROI — Minimal Rebuild Redundancy'` from Model 1.

For each remaining candidate, this model:
1. Detects the best filter column (timestamp/date) for the `is_incremental()` filter
2. Ranks unique key candidates by naming convention
3. Recommends an incremental strategy
4. Generates a copy-pasteable dbt config template

Uniqueness of key candidates is confirmed post-build by the `probe_unique_key_candidates()` post-hook, which uses `APPROXIMATE COUNT(DISTINCT ...)` against the actual table. The confirmed column populates `likely_unique_key` and is substituted into `dbt_config_template` and `validate_uniqueness_sql`.

---

## Strategy selection

Evaluated in priority order:

| Condition | Strategy |
|---|---|
| External deletes + filter column | `delete+insert` (flag for review) |
| External deletes, no filter | `merge` (flag for review) |
| Unique key + filter + large scale | `delete+insert` |
| Unique key + filter, moderate scale | `merge` |
| Filter only, no external DML | `append` |
| Filter only, external DML, no key | `append` (flag for manual review) |
| Unique key only, no filter | `merge` |
| No key or filter | `append` |

> **Note:** Redshift does not support dbt's `microbatch` incremental strategy. Large append-only tables that would receive `microbatch` on Snowflake receive `append` here instead.

---

## Key columns

| Column | Description |
|---|---|
| `incremental_strategy` | Recommended strategy: `append`, `merge`, or `delete+insert` |
| `suggested_filter_column` | Best timestamp/date column for the `is_incremental()` filter |
| `unique_key_candidates` | SUPER array of up to 3 plausible unique key candidates |
| `best_unique_key` | Top candidate by naming convention (unverified) |
| `likely_unique_key` | Confirmed by cardinality probe post-hook (null until macro runs) |
| `strategy_notes` | Human-readable rationale including caveats |
| `validate_uniqueness_sql` | SQL to run before implementing to verify the unique key |
| `dbt_config_template` | Copy-pasteable `config()` block + `is_incremental()` filter |

---

## Column naming conventions for key detection

**Filter columns** (ranked by suitability):
1. `*updated_at*`, `*modified_at*`
2. `*loaded_at*`, `*ingested_at*`, `*inserted_at*`, `*synced_at*`
3. `*created_at*`, `*event_date*`, `*event_time*`, `*event_timestamp*`
4. Any other timestamp or date column

**Unique key candidates** (ranked by naming convention):
1. `surrogate_key`, `primary_key`
2. Columns ending in `_sk`
3. `id`
4. Columns ending in `_id`
5. Columns ending in `_key`

---

## Key variables

| Variable | Default | Description |
|---|---|---|
| `incremental_large_table_row_threshold` | 10,000,000 | Row threshold for large-scale strategy selection |
| `incremental_large_table_gb_threshold` | 10 | GB threshold for large-scale strategy selection |
| `incremental_unique_key_probe_threshold` | 0.95 | Min `APPROX_COUNT_DISTINCT / COUNT(*)` ratio to confirm uniqueness |

---

## Implementing a recommendation

1. Check `likely_unique_key` — if populated, uniqueness is confirmed.
2. Run `validate_uniqueness_sql` on your table to double-check before implementing.
3. Copy `dbt_config_template` into your model file.
4. Test with `dbt build --select <model_name>` before promoting to production.
5. If no single-column key is confirmed, consider `dbt_utils.generate_surrogate_key([<grain_columns>])` to create a surrogate key.
