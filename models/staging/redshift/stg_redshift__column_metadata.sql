with source as (

    select
        lower(table_catalog)    as database_name,
        lower(table_schema)     as schema_name,
        lower(table_name)       as table_name,
        lower(column_name)      as column_name,
        lower(data_type)        as data_type,
        ordinal_position,
        case when lower(is_nullable) = 'yes' then true else false end as is_nullable

    from {{ source('redshift_usage', 'column_metadata') }}

)

select * from source
