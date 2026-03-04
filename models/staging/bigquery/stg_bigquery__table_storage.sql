{{ config(
    materialized='view'
) }}

select
    project_id,
    project_number,
    table_catalog,
    table_schema,
    table_name,
    creation_time,
    total_rows,
    total_partitions,
    total_logical_bytes,
    active_logical_bytes,
    long_term_logical_bytes,
    total_physical_bytes,
    active_physical_bytes,
    long_term_physical_bytes,
    time_travel_physical_bytes,
    deleted
from {{ source('bigquery_region_info', 'TABLE_STORAGE') }}
