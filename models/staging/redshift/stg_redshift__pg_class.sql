{#
  Passthrough of pg_catalog.pg_class — one row per relation (table, view,
  materialized view, index, sequence, etc.) in the current database.

  Leader-node only. Downstream models that join this against compute-node data
  must materialize as table at the join layer. OID columns are cast to bigint
  because Redshift cannot store the native `oid` type in user tables — leaving
  it as oid causes CTAS to fail with "unsupported type 'oid'" (SQLSTATE 0A000).
#}

with source as (

    select
        oid::bigint     as relation_id,
        relname         as relation_name,
        relnamespace::bigint as schema_id,
        relkind         as relation_kind

    from {{ source('redshift_usage', 'pg_class') }}

)

select * from source
