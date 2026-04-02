{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['table_fqn', 'column_name'],
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  Persistent store for column-level cardinality data produced by the
  refresh_column_cardinality macro.

  dbt owns the table schema and creates it on first run (empty). All data
  is written and maintained exclusively by the refresh_column_cardinality
  macro, which runs as a post-hook on fct_snowflake__table_clustering_candidates.
  dbt never inserts rows into this table directly — the incremental model
  always returns 0 rows, preserving macro-written data across runs.

  On first run: empty table with the correct schema is created.
  On subsequent runs: dbt merges 0 rows (no-op), macro data is preserved.
  After fct_snowflake__table_clustering_candidates runs: macro populates
  cardinality for the top N candidate tables.
--#}

select
    cast(null as varchar)       as table_fqn,
    cast(null as varchar)       as column_name,
    cast(null as bigint)        as distinct_values,
    cast(null as bigint)        as total_rows,
    cast(null as timestamp_ntz) as calculated_at
where 1 = 0
