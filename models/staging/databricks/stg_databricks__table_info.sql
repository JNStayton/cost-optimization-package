{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('databricks_storage', 'table_metrics_history') }}
)

, latest as (
    select *
    from source
    qualify row_number() over (partition by table_id order by snapshot_date desc) = 1
)

select
    catalog_name,
    schema_name,
    table_name,
    table_id,
    table_type,
    active_bytes as data_size_bytes,
    active_files as file_count,
    table_creation_time as created_at,
    table_dropped_time as deleted_at,
    snapshot_date,
    predictive_optimization_enabled,
    cast(null as string) as data_source_format,
    cast(null as bigint) as row_count,
    cast(null as array<string>) as partition_columns
from latest
