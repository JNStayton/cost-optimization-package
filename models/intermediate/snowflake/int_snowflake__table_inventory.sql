{{
  config(
    materialized='view',
    enabled=(target.type == 'snowflake')
  )
}}

select
    t.platform,
    upper(t.database_name) as database_name,
    upper(t.schema_name) as schema_name,
    upper(t.table_name) as table_name,
    upper(t.database_name) || '.' || upper(t.schema_name) || '.' || upper(t.table_name) as table_fqn,
    t.table_type,
    case
        when t.table_type = 'MATERIALIZED VIEW' then 'Materialized View'
        when t.is_transient then 'Transient Table'
        else 'Permanent Table'
    end as normalized_table_type,
    t.row_count,
    t.clustering_key,
    t.clustering_key is not null as is_already_clustered,
    t.is_transient,
    s.active_bytes,
    s.active_bytes / power(1024, 3) as size_gb,
    s.active_bytes / (16 * 1024 * 1024) as approx_micropartitions
from {{ ref('int_snowflake__tables') }} as t
inner join {{ ref('int_snowflake__table_storage') }} as s
    on upper(t.database_name) = upper(s.database_name)
    and upper(t.schema_name) = upper(s.schema_name)
    and upper(t.table_name) = upper(s.table_name)
where t.table_type in ('BASE TABLE', 'MATERIALIZED VIEW')
    and not t.is_deleted
    and not s.is_deleted
