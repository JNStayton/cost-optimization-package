{{
  config(
    materialized='view',
  )
}}

{#--
  Classifies queries by workload type using dbt query comment metadata
  and query_type. One row per query_id.

  Classification priority:
    1. dbt test (node_id starts with 'test.')
    2. Package internal (node_id matches this package)
    3. dbt model build (node_id starts with 'model.' or DML query types)
    4. Consumption (SELECT without dbt metadata)
    5. Unknown (everything else)

  Used by clustering pipeline to separate consumption scans (for key
  evaluation) from build/test scans (for materialization decisions).
--#}

select
    query_id,
    query_parameterized_hash,
    query_start_time,
    query_type,
    dbt_node_id,
    bytes_scanned,
    partitions_scanned,
    case
        when dbt_node_id like 'test.%'
            then 'dbt_test'
        when dbt_node_id like 'model.dbt_cost_optimization_package.%'
            then 'package_internal'
        when dbt_node_id like 'model.%'
            then 'dbt_model_build'
        when query_type in ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'DELETE', 'UPDATE')
            then 'dbt_model_build'
        when query_type = 'SELECT' and dbt_node_id is null
            then 'consumption'
        else 'unknown'
    end as workload_class,
    case
        when dbt_node_id is not null then 'high'
        when query_type in ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'DELETE', 'UPDATE') then 'high'
        when query_type = 'SELECT' and dbt_node_id is null then 'medium'
        else 'low'
    end as classification_confidence,
    split_part(coalesce(dbt_node_id, ''), '.', 1) as dbt_resource_type
from {{ ref('int_snowflake__query_history') }}
