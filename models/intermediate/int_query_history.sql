{#-- Routes to the platform-specific query history intermediate model and
    normalizes to the cross-platform int_query_history contract:
    query_id, query_start_time, query_hash, warehouse_name, warehouse_size,
    total_elapsed_time_ms, bytes_spilled_local, bytes_spilled_remote, query_text,
    session_id, execution_status, bytes_scanned, query_load_percent,
    queued_overload_time_ms, statement_type, execution_time_ms,
    partitions_scanned, partitions_total, platform. --#}

{% if target.type == 'snowflake' %}

select * from {{ ref('int_snowflake__query_history') }}

{% elif target.type == 'bigquery' %}

select * from {{ ref('int_bigquery__query_history') }}

{% elif target.type == 'redshift' %}

select
    query_id,
    start_time                                          as query_start_time,
    generic_query_hash                                  as query_hash,
    cast(null as varchar)                               as warehouse_name,
    cast(null as varchar)                               as warehouse_size,
    cast(elapsed_time_seconds * 1000 as bigint)         as total_elapsed_time_ms,
    cast(0 as bigint)                                   as bytes_spilled_local,
    cast(0 as bigint)                                   as bytes_spilled_remote,
    query_text,
    session_id,
    upper(execution_status)                             as execution_status,
    cast(null as bigint)                                as bytes_scanned,
    cast(null as numeric(5,2))                          as query_load_percent,
    cast(queue_time_seconds * 1000 as bigint)           as queued_overload_time_ms,
    upper(query_type)                                   as statement_type,
    cast(execution_time_seconds * 1000 as bigint)       as execution_time_ms,
    cast(null as bigint)                                as partitions_scanned,
    cast(null as bigint)                                as partitions_total,
    'redshift'                                          as platform

from {{ ref('int_redshift__query_history') }}

{% elif target.type == 'databricks' %}

select * from {{ ref('int_databricks__query_history') }}

{% else %}

{{ exceptions.raise_compiler_error(
  "Unsupported adapter type: " ~ target.type ~
  ". Supported adapters are: snowflake, bigquery, databricks, redshift."
) }}

{% endif %}
