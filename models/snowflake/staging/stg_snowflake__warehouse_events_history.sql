{{ config(
    materialized='incremental',
    unique_key='event_key',
    on_schema_change='append_new_columns'
) }}

select
    md5(
        coalesce(to_varchar(timestamp), '') || '|' ||
        coalesce(to_varchar(warehouse_id), '') || '|' ||
        coalesce(event_name, '') || '|' ||
        coalesce(to_varchar(cluster_number), '0')
    ) as event_key,
    timestamp as event_timestamp,
    warehouse_id,
    warehouse_name,
    cluster_number,
    event_name,
    event_reason,
    event_state,
    size as warehouse_size,
    cluster_count,
    warehouse_type,
    resource_constraint
from {{ source('snowflake_usage', 'warehouse_events_history') }}
where event_name in (
    'WAREHOUSE_CONSISTENT',
    'SPINUP_CLUSTER',
    'RESUME_CLUSTER',
    'CREATE_WAREHOUSE',
    'ALTER_WAREHOUSE'
)
{% if is_incremental() %}
    and timestamp >= (select dateadd(day, -7, max(event_timestamp)) from {{ this }})
{% endif %}
