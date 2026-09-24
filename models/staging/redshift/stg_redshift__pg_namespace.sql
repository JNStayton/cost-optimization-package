{#
  Passthrough of pg_catalog.pg_namespace — one row per schema in the current database.
  Leader-node only.
#}

with source as (

    select
        oid::bigint     as schema_id,
        nspname         as schema_name

    from {{ source('redshift_usage', 'pg_namespace') }}

)

select * from source
