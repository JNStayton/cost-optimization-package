select
    job_id as query_id,
    creation_time as query_start_time,
    total_bytes_billed as bytes_scanned,
    statement_type,
    total_slot_ms as execution_time_ms,
    query as query_text,
    referenced_tables,
    destination_table,
    case
        when state = 'DONE' and error_result is null then 'SUCCESS'
        when state = 'DONE' and error_result is not null then 'FAILED'
        else upper(state)
    end as execution_status,
    'bigquery' as platform
from {{ ref('stg_bigquery__jobs_by_project') }}
