{{ config(
    materialized='view'
) }}

select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    is_partitioning_column,
    clustering_ordinal_position
from {{ source('bigquery_dataset_info', 'INFORMATION_SCHEMA_COLUMNS') }}
