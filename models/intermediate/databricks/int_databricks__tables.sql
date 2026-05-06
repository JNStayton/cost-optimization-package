select
    catalog_name as database_name,
    schema_name,
    table_name,
    table_id,
    case
        when table_type = 'MANAGED_TABLE' then 'BASE TABLE'
        when table_type = 'EXTERNAL_TABLE' then 'EXTERNAL TABLE'
        else table_type
    end as table_type,
    row_count,
    case
        when partition_columns is not null and size(partition_columns) > 0
        then array_join(partition_columns, ', ')
        else null
    end as clustering_key,
    cast(false as boolean) as is_transient,
    case when deleted_at is not null then true else false end as is_deleted,
    'databricks' as platform
from {{ ref('stg_databricks__table_info') }}
