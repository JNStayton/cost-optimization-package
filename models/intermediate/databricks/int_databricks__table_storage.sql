select
    catalog_name as database_name,
    schema_name,
    table_name,
    data_size_bytes as active_bytes,
    cast(null as bigint) as time_travel_bytes,
    cast(null as bigint) as failsafe_bytes,
    file_count,
    case when deleted_at is not null then true else false end as is_deleted,
    'databricks' as platform
from {{ ref('stg_databricks__table_info') }}
