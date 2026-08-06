{{ config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns',
    cluster_by=['to_date(start_time)']
) }}

select
    query_id, 
    start_time, 
    query_hash,
    query_parameterized_hash,
    user_name,
    role_name,
    warehouse_name, 
    warehouse_size, 
    total_elapsed_time, 
    bytes_scanned, 
    query_load_percent, 
    queued_overload_time,
    queued_provisioning_time,
    query_type, 
    execution_time, 
    partitions_scanned, 
    partitions_total,
    bytes_spilled_to_local_storage, 
    bytes_spilled_to_remote_storage, 
    query_text, 
    session_id,
    execution_status,
    rows_inserted,
    -- dbt Cloud context parsed from query comment
    parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):dbt_cloud_run_id::string as dbt_cloud_run_id,
    parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):dbt_cloud_job_id::string as dbt_cloud_job_id,
    parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):node_id::string as dbt_node_id,
    parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):target_name::string as dbt_target_name,
    parse_json(regexp_substr(query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):dbt_cloud_environment_id::string as dbt_cloud_environment_id
from {{ source('snowflake_usage', 'query_history') }}
where execution_status = 'SUCCESS'
{% if is_incremental() %}
  and start_time >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(start_time)) from {{ this }})
{% else %}
  and start_time >= dateadd(day, -{{ var('incremental_overlap_days', 31) }}, current_timestamp())
{% endif %}