{{
    config(
        materialized='table'
    )
}}

{#
  SQL-based DAG edge table: one row per direct parent → child model dependency.

  Reads from int_dbt__relations (already materialized at compile time), unnesting
  the parent_models SUPER array using Redshift's native SUPER unnesting syntax —
  the Redshift equivalent of lateral flatten in Snowflake.

  NOTE: int_redshift__view_chains uses WITH RECURSIVE against this table.
  Do NOT add ephemeral model refs to int_redshift__view_chains — dbt would inline them
  as non-recursive CTEs before the query, stripping the RECURSIVE keyword and breaking
  the transitive closure computation.
#}

with child_relations as (

    select
        lower(r.database_name)          as child_database,
        lower(r.schema_name)            as child_schema,
        lower(r.table_name)             as child_name,
        lower(r.materialized)           as child_materialization,
        lower(parent_fqn::varchar)      as parent_fqn

    from {{ ref('int_dbt__relations') }} r, r.parent_models parent_fqn

),

parent_lookup as (

    select
        lower(database_name) || '.' || lower(schema_name) || '.' || lower(table_name)
                                        as parent_fqn,
        lower(database_name)            as parent_database,
        lower(schema_name)              as parent_schema,
        lower(table_name)               as parent_name,
        lower(materialized)             as parent_materialization

    from {{ ref('int_dbt__relations') }}

)

select
    pl.parent_database,
    pl.parent_schema,
    pl.parent_name,
    pl.parent_materialization,
    cr.child_database,
    cr.child_schema,
    cr.child_name,
    cr.child_materialization

from child_relations cr
inner join parent_lookup pl
    on cr.parent_fqn = pl.parent_fqn
