{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['table_fqn', 'column_name'],
    on_schema_change='sync_all_columns',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  This model returns 0 rows by design — the incremental merge is a no-op,
  preserving macro-written data across dbt runs.
--#}

select
    cast(null as string)    as table_fqn,
    cast(null as string)    as column_name,
    cast(null as int64)     as distinct_values,
    cast(null as int64)     as total_rows,
    cast(null as timestamp) as calculated_at
limit 0
