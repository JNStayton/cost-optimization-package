{{ config(
    materialized='view',
    enabled=(target.type == 'databricks')
) }}

with dbt_queries as (
    select
        statement_id,
        statement_text,
        start_time,
        total_duration_ms as execution_time_ms,
        coalesce(read_bytes, 0) as bytes_scanned,
        regexp_extract(statement_text, '"node_id":\\s*"([^"]+)"', 1) as node_id
    from {{ ref('stg_databricks__query_history') }}
    where statement_text ilike '%"app": "dbt"%'
        and execution_status = 'FINISHED'
        and statement_text is not null
)

select
    dq.statement_id,
    dq.start_time,
    dq.execution_time_ms,
    dq.bytes_scanned,
    dq.node_id,
    dr.model_name,
    dr.materialized,
    dr.database_name,
    dr.schema_name,
    dr.table_name,
    dr.database_name || '.' || dr.schema_name || '.' || dr.table_name as table_fqn
from dbt_queries as dq
inner join {{ ref('int_dbt__relations') }} as dr
    on dq.node_id = dr.dbt_model
where dq.node_id != ''
