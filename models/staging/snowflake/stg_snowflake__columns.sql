{{
  config(
    materialized='view',
    enabled=(target.type == 'snowflake')
  )
}}

select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    character_maximum_length,
    numeric_precision,
    is_nullable,
    deleted
from {{ source('snowflake_usage', 'columns') }}
