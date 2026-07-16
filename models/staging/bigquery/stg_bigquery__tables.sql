{{ config(
    materialized='view'
) }}

select
    table_catalog,
    table_schema,
    table_name,
    table_type,
    creation_time,
    base_table_catalog,
    base_table_schema,
    base_table_name,
    snapshot_time_ms,
    ddl
from {{ source('bigquery_dataset_info', 'INFORMATION_SCHEMA_TABLES') }}
