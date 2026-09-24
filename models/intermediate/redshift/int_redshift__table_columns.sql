{{ config(materialized='table') }}

{# Column-level metadata scoped to tables present in int_dbt__relations (dbt-managed models only).
   Mirrors the output shape of int_snowflake__table_columns for cross-platform parity. #}

with column_metadata as (

    select * from {{ ref('int_redshift__column_metadata') }}

),

dbt_relations as (

    select
        lower(database_name)    as database_name,
        lower(schema_name)      as schema_name,
        lower(table_name)       as table_name

    from {{ ref('int_dbt__relations') }}

),

filtered as (

    select
        cm.database_name,
        cm.schema_name,
        cm.table_name,
        cm.database_name || '.' || cm.schema_name || '.' || cm.table_name  as table_fqn,
        cm.column_name,
        cm.ordinal_position,
        cm.data_type,
        cm.is_nullable

    from column_metadata cm
    inner join dbt_relations dr
        on cm.database_name = dr.database_name
        and cm.schema_name  = dr.schema_name
        and cm.table_name   = dr.table_name

)

select * from filtered
