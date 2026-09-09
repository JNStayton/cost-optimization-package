{{ config(
    materialized='incremental',
    unique_key='statement_id'
) }}

{% if var('use_mock_data', false) %}

select
    statement_id,
    session_id,
    executed_by,
    statement_text,
    statement_type,
    execution_status,
    warehouse_id,
    compute_type,
    client_application,
    start_time,
    end_time,
    total_duration_ms,
    waiting_at_capacity_duration_ms,
    waiting_for_compute_duration_ms,
    execution_duration_ms,
    compilation_duration_ms,
    result_fetch_duration_ms,
    read_bytes,
    spilled_local_bytes,
    produced_rows,
    read_files,
    read_partitions,
    from_result_cache
from {{ ref('databricks_query_history') }}

{% else %}

select
    statement_id,
    session_id,
    executed_by,
    statement_text,
    statement_type,
    execution_status,
    compute.warehouse_id,
    compute.type as compute_type,
    client_application,
    start_time,
    end_time,
    total_duration_ms,
    waiting_at_capacity_duration_ms,
    waiting_for_compute_duration_ms,
    execution_duration_ms,
    compilation_duration_ms,
    result_fetch_duration_ms,
    read_bytes,
    spilled_local_bytes,
    produced_rows,
    read_files,
    read_partitions,
    from_result_cache
from {{ source('databricks_query', 'history') }}

{% if is_incremental() %}
  where start_time >= (select max(start_time) from {{ this }}) - INTERVAL 7 DAYS
{% endif %}

{% endif %}
