{#--
  BigQuery table storage metrics. Passes through total_rows and total_partitions
  from stg_bigquery__table_storage for use by int_bigquery__table_inventory.

  Note: total_rows and total_partitions are BigQuery-specific extra columns beyond
  the standard int_table_storage schema. Column names follow Snowflake convention;
  a future refactor could make them platform-neutral.
--#}
select
    project_id as database_name,
    table_schema as schema_name,
    table_name,
    active_physical_bytes as active_bytes,
    time_travel_physical_bytes as time_travel_bytes,
    -- BigQuery has no failsafe storage concept (Snowflake-only)
    cast(null as int64) as failsafe_bytes,
    deleted as is_deleted,
    -- BigQuery-specific fields used by int_bigquery__table_inventory
    total_rows,
    total_partitions,
    'bigquery' as platform
from {{ ref('stg_bigquery__table_storage') }}
