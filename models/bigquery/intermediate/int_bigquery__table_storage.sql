select
    project_id as database_name,
    table_schema as schema_name,
    table_name,
    active_physical_bytes as active_bytes,
    deleted as is_deleted,
    total_rows,
    total_partitions,
    'bigquery' as platform
from {{ ref('stg_bigquery__table_storage') }}
