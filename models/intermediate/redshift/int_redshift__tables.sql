select
    database_name,
    schema_name,
    table_name,
    table_id::varchar as table_id,
    cast('BASE TABLE' as varchar) as table_type,
    total_rows as row_count,
    sortkey1 as clustering_key,
    cast(false as boolean) as is_transient,
    cast(false as boolean) as is_deleted,
    'redshift' as platform
from {{ ref('stg_redshift__table_info') }}
