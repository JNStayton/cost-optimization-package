# BigQuery Clustering Key Candidates

`fct_bigquery__clustering_key_candidates` produces daily column-level clustering key
recommendations for BigQuery tables identified as candidates by
`fct_bigquery__table_clustering_candidates`. Output mirrors
`fct_snowflake__clustering_key_candidates`; only the underlying signal sources differ.

## Pipeline

```
stg_bigquery__columns
  → int_bigquery__table_columns
  → int_bigquery__column_query_access  (when use_query_text_attribution = true)
       → int_bigquery__column_query_stats
fct_bigquery__table_clustering_candidates
  → post-hook: refresh_bigquery_column_cardinality()
       → int_bigquery__column_cardinality
fct_bigquery__clustering_key_candidates
```

The post-hook on the table-level fact populates `int_bigquery__column_cardinality`
before the column-level fact reads from it. Build order is guaranteed because the
column-level fact `ref()`s the table-level fact.

## Variables

| Variable | Default | Effect |
|---|---|---|
| `clustering_candidates_lookback_days` | `7` | Lookback window for query-text and column-query-stats reads. |
| `clustering_key_cardinality_table_limit` | `10` | Max number of candidate tables for which APPROX_COUNT_DISTINCT is computed each run. |
| `use_query_text_attribution` | `true` | When `false`, disables the heuristic column-access pipeline. `usage_count` becomes 0 and ranking falls back to cardinality only. |

Override via `dbt_project.yml`:

```yaml
vars:
  clustering_candidates_lookback_days: 14
  clustering_key_cardinality_table_limit: 25
  use_query_text_attribution: true
```

## BigQuery-specific caveats

1. **`is_recommended` ranks 1-4** (Snowflake uses 1-3). BigQuery supports up to four
   clustering columns; the column with the highest `column_score` should be listed
   first in the `CLUSTER BY` clause.

2. **Partitioning columns are excluded.** `int_bigquery__table_columns` filters out
   columns where `is_partitioning_column = 'YES'` — partitioning prunes before
   clustering, so adding a partition column to `CLUSTER BY` is wasted work.

3. **Eligible data types** are restricted to BigQuery's clustering allow-list:
   `BIGNUMERIC`, `BOOL`, `DATE`, `DATETIME`, `GEOGRAPHY`, `INT64`, `NUMERIC`,
   `RANGE`, `STRING`, `TIMESTAMP`. `FLOAT64`, `BYTES`, `JSON`, `ARRAY`, `STRUCT`,
   and `TIME` are excluded.

4. **`usage_count` is heuristic.** BigQuery's `JOBS_BY_PROJECT` does not expose
   column-level lineage, so we regex-extract substrings rooted at `WHERE`,
   `JOIN ON`, and `ORDER BY` clauses from `query_text` and word-boundary-match
   column names. This is materially less precise than Snowflake's
   `ACCESS_HISTORY.columns[]`. Known false-positive: a column name appearing in a
   comment inside a filter clause will be attributed.

## Sample queries

Top 4 recommendations per table for today's snapshot:

```sql
select
    table_fqn,
    column_name,
    recommended_key_position,
    column_score,
    cardinality_pct,
    usage_count
from {{ ref('fct_bigquery__clustering_key_candidates') }}
where snapshot_date = current_date()
    and is_recommended
order by table_fqn, recommended_key_position;
```

Tables where the recommendation disagrees with the existing clustering key:

```sql
with recs as (
    select
        table_fqn,
        string_agg(column_name, ', ' order by recommended_key_position) as recommended_key
    from {{ ref('fct_bigquery__clustering_key_candidates') }}
    where snapshot_date = current_date()
        and is_recommended
    group by table_fqn
)
select
    t.table_fqn,
    t.clustering_key as current_key,
    r.recommended_key
from {{ ref('int_bigquery__tables') }} as t
join recs as r on r.table_fqn = (t.database_name || '.' || t.schema_name || '.' || t.table_name)
where t.clustering_key is not null
    and t.clustering_key != r.recommended_key;
```
