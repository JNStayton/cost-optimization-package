{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='operator_column_key',
    on_schema_change='append_new_columns',
  )
}}

{#--
  Stores parsed column references from query plan operators (GET_QUERY_OPERATOR_STATS).
  One row per (query_id, table_fqn, column_name, operator_type).

  operator_type values:
    - 'Filter'  : column appeared in a WHERE/HAVING condition
    - 'Join'    : column appeared in a JOIN equality condition

  This table is populated by the extract_query_operator_columns macro
  (run via dbt run-operation or post-hook). The model itself only defines
  the schema and handles full-refresh resets.

  GET_QUERY_OPERATOR_STATS has 14-day retention, so this table accumulates
  history beyond that window. The macro handles deduplication on insert.
--#}

{% if is_incremental() %}
    -- Incremental runs are no-ops; the macro handles inserts directly.
    -- This query returns zero rows but maintains the schema contract.
    select
        cast(null as varchar) as operator_column_key,
        cast(null as varchar) as query_id,
        cast(null as varchar) as table_fqn,
        cast(null as varchar) as column_name,
        cast(null as varchar) as operator_type,
        cast(null as timestamp_ntz) as query_start_time,
        cast(null as date) as access_date
    where 1 = 0
{% else %}
    -- Full refresh: create empty table with correct schema.
    select
        cast(null as varchar) as operator_column_key,
        cast(null as varchar) as query_id,
        cast(null as varchar) as table_fqn,
        cast(null as varchar) as column_name,
        cast(null as varchar) as operator_type,
        cast(null as timestamp_ntz) as query_start_time,
        cast(null as date) as access_date
    where 1 = 0
{% endif %}
