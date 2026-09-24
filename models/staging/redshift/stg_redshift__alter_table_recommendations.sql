with source as (

    select
        trim(type)              as recommendation_type,
        trim("database")        as database_name,
        -- cast oid to bigint — Redshift CTAS rejects the native `oid` type
        table_id::bigint        as table_id,
        group_id,
        trim(ddl)               as recommended_ddl,
        case
            when trim(auto_eligible) = 't' then true
            else false
        end                     as is_auto_eligible

    from {{ source('redshift_usage', 'alter_table_recommendations') }}

)

select * from source
