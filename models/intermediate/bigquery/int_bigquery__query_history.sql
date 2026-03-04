select
    job_id as query_id,
    creation_time as query_start_time,
    to_hex(md5(query)) as query_hash,
    reservation_id as warehouse_name,
    cast(null as string) as warehouse_size,
    timestamp_diff(end_time, start_time, millisecond) as total_elapsed_time_ms,
    total_bytes_billed as bytes_scanned,
    cast(null as float64) as query_load_percent,
    cast(null as int64) as queued_overload_time_ms,
    statement_type,
    total_slot_ms as execution_time_ms,
    cast(null as int64) as partitions_scanned,
    cast(null as int64) as partitions_total,
    cast(null as int64) as bytes_spilled_local,
    cast(null as int64) as bytes_spilled_remote,
    query as query_text,
    cast(null as string) as session_id,
    case
        when state = 'DONE' and error_result is null then 'SUCCESS'
        when state = 'DONE' and error_result is not null then 'FAILED'
        else upper(state)
    end as execution_status,
    'bigquery' as platform
from {{ ref('stg_bigquery__jobs_by_project') }}
