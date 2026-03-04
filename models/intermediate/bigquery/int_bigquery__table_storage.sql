select
    project_id as database_name,
    table_schema as schema_name,
    table_name,
    active_physical_bytes as active_bytes,
    time_travel_physical_bytes as time_travel_bytes,
    cast(null as int64) as failsafe_bytes,
    deleted as is_deleted,
    'bigquery' as platform
from {{ ref('stg_bigquery__table_storage') }}
