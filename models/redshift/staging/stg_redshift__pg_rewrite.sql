{#
  Passthrough of pg_catalog.pg_rewrite — rewrite rules, including the rules that
  define each view. Each view has one rule whose ev_class points to the view itself.
  Joined with pg_depend to extract the base relations a view references.
  Leader-node only.
#}

with source as (

    select
        oid::bigint         as rule_id,
        ev_class::bigint    as view_relation_id

    from {{ source('redshift_usage', 'pg_rewrite') }}

)

select * from source
