select
    statement_id as query_id,
    start_time as query_start_time,
    md5(statement_text) as query_hash,
    warehouse_id as warehouse_name,
    compute_type as warehouse_size,
    total_duration_ms as total_elapsed_time_ms,
    read_bytes as bytes_scanned,
    cast(null as double) as query_load_percent,
    waiting_at_capacity_duration_ms as queued_overload_time_ms,
    statement_type,
    execution_duration_ms as execution_time_ms,
    cast(read_partitions as bigint) as partitions_scanned,
    cast(read_files as bigint) as partitions_total,
    spilled_local_bytes as bytes_spilled_local,
    cast(null as bigint) as bytes_spilled_remote,
    statement_text as query_text,
    session_id,
    case
        when execution_status = 'FINISHED' then 'SUCCESS'
        when execution_status = 'FAILED' then 'FAILED'
        when execution_status = 'CANCELED' then 'CANCELLED'
        else upper(execution_status)
    end as execution_status,
    'databricks' as platform
from {{ ref('stg_databricks__query_history') }}
