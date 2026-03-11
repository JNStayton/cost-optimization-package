select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    coalesce(deleted, false) as is_deleted,
    'snowflake' as platform
from {{ ref('stg_snowflake__table_storage_metrics') }}