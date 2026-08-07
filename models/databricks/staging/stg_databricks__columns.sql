{{ config(materialized='view') }}

select
    table_catalog as catalog_name,
    table_schema  as schema_name,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable
from {{ source('databricks_information_schema', 'columns') }}
where table_schema != 'information_schema'
    and table_catalog != 'system'
