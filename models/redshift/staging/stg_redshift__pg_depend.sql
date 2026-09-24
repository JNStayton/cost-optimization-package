{#
  Passthrough of pg_catalog.pg_depend — dependency edges between database objects.
  Used (joined with pg_rewrite) to map views to the relations they reference.
  Leader-node only.
#}

with source as (

    select
        classid::bigint     as dependent_class_id,
        objid::bigint       as dependent_object_id,
        refclassid::bigint  as referenced_class_id,
        refobjid::bigint    as referenced_object_id,
        refobjsubid         as referenced_object_sub_id,
        deptype             as dependency_type

    from {{ source('redshift_usage', 'pg_depend') }}

)

select * from source
