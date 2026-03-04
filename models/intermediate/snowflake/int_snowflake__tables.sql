select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    table_id::string as table_id,
    table_type,
    row_count,
    clustering_key,
    case when is_transient = 'YES' then true else false end as is_transient,
    case when deleted is not null then true else false end as is_deleted,
    'snowflake' as platform
from {{ ref('stg_snowflake__tables') }}