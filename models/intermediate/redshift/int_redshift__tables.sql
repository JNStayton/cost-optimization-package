with tables as (

    select
        database_name,
        schema_name,
        table_name,
        table_type

    from {{ ref('stg_redshift__tables') }}
    -- exclude system schemas that svv_tables exposes but svv_table_info naturally omits
    where schema_name not in ('pg_catalog', 'information_schema', 'pg_internal', 'catalog_history')

),

table_stats as (

    select
        database_name,
        schema_name,
        table_name,
        table_id,
        total_rows,
        sortkey1

    from {{ ref('int_redshift__table_info') }}

)

select
    'redshift'                          as platform,
    t.database_name,
    t.schema_name,
    t.table_name,
    cast(ts.table_id as varchar)        as table_id,
    t.table_type,
    ts.total_rows                       as row_count,
    ts.sortkey1                         as clustering_key,
    cast(false as boolean)              as is_transient,
    cast(false as boolean)              as is_deleted

from tables t
left join table_stats ts
    on upper(t.database_name)   = upper(ts.database_name)
    and upper(t.schema_name)    = upper(ts.schema_name)
    and upper(t.table_name)     = upper(ts.table_name)
