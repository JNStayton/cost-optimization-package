{{ config(
    materialized='incremental',
    unique_key='job_id'
) }}

select
    job_id,
    creation_time,
    start_time,
    end_time,
    total_slot_ms,
    total_bytes_processed,
    total_bytes_billed,
    cache_hit,
    query,
    statement_type,
    user_email,
    state,
    error_result,
    reservation_id,
    bi_engine_statistics
from {{ source('bigquery_region_info', 'JOBS_BY_PROJECT') }}

{% if is_incremental() %}
  where creation_time >= (select timestamp_sub(max(creation_time), interval 7 day) from {{ this }})
{% endif %}
