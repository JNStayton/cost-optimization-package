{{ config(
    materialized='view'
) }}

select
    project_id,
    table_schema,
    table_name,
    total_rows,
    total_partitions,
    active_physical_bytes,
    deleted
from {{ source('bigquery_region_info', 'TABLE_STORAGE') }}
