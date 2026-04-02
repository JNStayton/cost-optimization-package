{{
  config(
    materialized='view',
    enabled=(target.type == 'snowflake')
  )
}}


{#--
  General-purpose column metadata for all non-deleted Snowflake tables, scoped
  to tables that exist in int_snowflake__table_inventory (active, non-deleted
  base tables and materialized views). No clustering-specific filtering applied —
  downstream clustering models apply their own data type exclusions.
--#}

select
    upper(c.database_name) as database_name,
    upper(c.schema_name) as schema_name,
    upper(c.table_name) as table_name,
    upper(c.database_name) || '.' || upper(c.schema_name) || '.' || upper(c.table_name) as table_fqn,
    c.column_name,
    c.ordinal_position,
    c.data_type,
    c.is_nullable
from {{ ref('stg_snowflake__columns') }} as c
inner join {{ ref('int_snowflake__table_inventory') }} as ti
    on upper(c.database_name) = upper(ti.database_name)
    and upper(c.schema_name) = upper(ti.schema_name)
    and upper(c.table_name) = upper(ti.table_name)
where c.deleted is null
