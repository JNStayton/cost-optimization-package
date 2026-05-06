{{ config(
    materialized='view'
) }}

select
    table_catalog,
    table_schema,
    table_name,
    table_type,
    data_source_format,
    is_insertable_into,
    created,
    last_altered,
    table_owner,
    comment
from {{ source('databricks_information_schema', 'tables') }}
