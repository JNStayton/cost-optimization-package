{{ config(
    materialized='view',
    enabled=(target.type == 'snowflake')
) }}

select
    table_id,
    table_catalog,
    table_schema,
    table_name,
    table_type,
    row_count,
    clustering_key,
    is_transient,
    deleted
from {{ source('snowflake_usage', 'tables') }}