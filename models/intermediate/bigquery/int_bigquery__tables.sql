select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    cast(null as string) as table_id,
    table_type,
    cast(null as int64) as row_count,
    cast(null as string) as clustering_key,
    cast(null as boolean) as is_transient,
    cast(null as boolean) as is_deleted,
    'bigquery' as platform
from {{ ref('stg_bigquery__tables') }}
