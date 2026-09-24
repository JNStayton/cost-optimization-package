{{ config(
    materialized='incremental',
    unique_key='job_id',
    on_schema_change='sync_all_columns'
) }}

select
    job_id,
    creation_time,
    total_slot_ms,
    total_bytes_billed,
    query,
    statement_type,
    state,
    error_result,
    referenced_tables,
    destination_table
from {{ source('bigquery_region_info', 'JOBS_BY_PROJECT') }}

{% if is_incremental() %}
  where creation_time >= (select timestamp_sub(max(creation_time), interval 7 day) from {{ this }})
{% endif %}
