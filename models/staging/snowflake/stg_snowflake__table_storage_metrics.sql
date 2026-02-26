{{ config(
    materialized='view',
    enabled=(target.type == 'snowflake')
) }}

select
    table_catalog,
    table_schema,
    table_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    retained_for_clone_bytes,
    deleted
from {{ source('snowflake_usage', 'table_storage_metrics') }}