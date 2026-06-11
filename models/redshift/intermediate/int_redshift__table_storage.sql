select
    database_name,
    schema_name,
    table_name,
    (size_mb * 1024 * 1024)::bigint as active_bytes,
    cast(null as bigint) as time_travel_bytes,
    cast(null as bigint) as failsafe_bytes,
    cast(false as boolean) as is_deleted,
    'redshift' as platform
from {{ ref('stg_redshift__table_info') }}
