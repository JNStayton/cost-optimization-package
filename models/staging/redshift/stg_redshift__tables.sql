{#
  Passthrough of svv_tables — one row per relation visible to the current user.
  Columns are listed explicitly (not `select *`) because Redshift's late-binding
  view resolution can fail to capture all columns from leader-only system views
  at view-creation time, surfacing as "column does not exist" errors at
  downstream query-prepare time.
#}

with source as (

    select
        table_catalog,
        table_schema,
        table_name,
        table_type

    from {{ source('redshift_usage', 'tables') }}

),

renamed as (

    select
        trim(table_catalog)     as database_name,
        trim(table_schema)      as schema_name,
        trim(table_name)        as table_name,
        trim(table_type)        as table_type

    from source

)

select * from renamed
