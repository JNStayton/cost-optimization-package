{{
  config(
    materialized='view',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  BigQuery column metadata, scoped to tables present in int_bigquery__table_inventory.

  Filters:
    - data_type must be in BigQuery's clustering allow-list
      (https://cloud.google.com/bigquery/docs/clustered-tables#cluster_column_types)
    - is_partitioning_column = 'NO' — partition columns are pruned before clustering,
      so adding them to a CLUSTER BY is wasted

  Column names follow Snowflake convention; passes clustering_ordinal_position through
  for observability (not used in scoring).
--#}

with eligible_columns as (
    select
        c.database_name,
        c.schema_name,
        c.table_name,
        c.database_name || '.' || c.schema_name || '.' || c.table_name as table_fqn,
        c.column_name,
        c.ordinal_position,
        c.data_type,
        c.is_nullable,
        c.clustering_ordinal_position
    from {{ ref('stg_bigquery__columns') }} as c
    where c.is_partitioning_column = 'NO'
        and c.data_type in (
            'INT64', 'INT', 'INTEGER',
            'NUMERIC', 'BIGNUMERIC',
            'STRING',
            'DATE', 'TIMESTAMP', 'DATETIME',
            'BOOL', 'BOOLEAN',
            'GEOGRAPHY'
        )
)

select
    ec.database_name,
    ec.schema_name,
    ec.table_name,
    ec.table_fqn,
    ec.column_name,
    ec.ordinal_position,
    ec.data_type,
    ec.is_nullable,
    ec.clustering_ordinal_position
from eligible_columns as ec
inner join {{ ref('int_bigquery__table_inventory') }} as ti
    on ec.database_name = ti.database_name
    and ec.schema_name = ti.schema_name
    and ec.table_name = ti.table_name
