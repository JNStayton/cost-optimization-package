select
    t.platform,
    upper(t.database_name)                                                              as database_name,
    upper(t.schema_name)                                                                as schema_name,
    upper(t.table_name)                                                                 as table_name,
    upper(t.database_name) || '.' || upper(t.schema_name) || '.' || upper(t.table_name)
                                                                                        as table_fqn,
    t.table_type,
    case
        when t.table_type = 'EXTERNAL TABLE'    then 'External Table'
        when t.table_type = 'SHARED TABLE'      then 'Shared Table'
        when t.table_type = 'VIEW'              then 'View'
        else                                         'Permanent Table'
    end                                                                                 as normalized_table_type,
    t.row_count,
    t.clustering_key,
    t.clustering_key is not null                                                        as is_already_clustered,
    t.is_transient,
    s.active_bytes,
    cast(s.active_bytes as float) / (1024.0 * 1024 * 1024)                             as size_gb,
    cast(null as bigint)                                                                as approx_micropartitions

from {{ ref('int_redshift__tables') }} as t
inner join {{ ref('int_redshift__table_storage') }} as s
    on upper(t.database_name)   = upper(s.database_name)
    and upper(t.schema_name)    = upper(s.schema_name)
    and upper(t.table_name)     = upper(s.table_name)
where t.table_type = 'BASE TABLE'
    and not t.is_deleted
    and not s.is_deleted
