{{ config(
    materialized='view'
) }}

select
    table_catalog,
    table_schema,
    table_name,
    table_type,
    ddl
from {{ source('bigquery_dataset_info', 'INFORMATION_SCHEMA_TABLES') }}
