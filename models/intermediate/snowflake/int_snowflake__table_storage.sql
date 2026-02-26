{#
  Maps Snowflake table storage metrics from staging to unified intermediate schema.
  Normalizes storage byte columns and adds is_deleted flag and platform identifier.
#}
{{ config(materialized='ephemeral') }}

select
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    active_bytes,
    time_travel_bytes,
    failsafe_bytes,
    case when deleted is not null then true else false end as is_deleted,
    'snowflake' as platform
from {{ ref('stg_snowflake__table_storage_metrics') }}