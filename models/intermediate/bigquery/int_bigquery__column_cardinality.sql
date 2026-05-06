{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['table_fqn', 'column_name'],
    on_schema_change='append_new_columns',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  Persistent store for column-level APPROX_COUNT_DISTINCT values produced by
  the refresh_bigquery_column_cardinality macro. dbt creates the empty table
  schema on first run; all data is written exclusively by the macro running as
  a post-hook on fct_bigquery__table_clustering_candidates.

  This model returns 0 rows by design — the incremental merge is a no-op,
  preserving macro-written data across dbt runs.
--#}

select
    cast(null as string)    as table_fqn,
    cast(null as string)    as column_name,
    cast(null as int64)     as distinct_values,
    cast(null as int64)     as total_rows,
    cast(null as timestamp) as calculated_at
where 1 = 0
