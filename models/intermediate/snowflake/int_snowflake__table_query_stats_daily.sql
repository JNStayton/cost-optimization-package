{#--
  Daily query stats per table. Attribution uses either access_history (Enterprise+)
  or query_text ILIKE (Standard). Set vars.use_access_history_attribution = false
  in dbt_project.yml for Standard edition (no ACCESS_HISTORY view).
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='table_query_stats_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake')
  )
}}

with candidate_tables as (
    select distinct
        platform,
        database_name as table_database,
        schema_name as table_schema,
        table_name
    from {{ ref('int_snowflake__table_inventory') }}
),

query_history as (
    select
        query_id,
        cast(query_start_time as date) as stats_date,
        query_start_time,
        statement_type,
        execution_time_ms,
        partitions_scanned,
        partitions_total,
        bytes_scanned,
        bytes_spilled_local,
        bytes_spilled_remote,
        query_text
    from {{ ref('int_snowflake__query_history') }}
    where execution_status = 'SUCCESS'
    {% if is_incremental() %}
        and query_start_time >= dateadd(
            day,
            -1,
            (
                select coalesce(max(stats_date), '1970-01-01'::date)
                from {{ this }}
            )
        )
    {% endif %}
),

{% if var('use_access_history_attribution', true) %}
query_table_access as (
    select
        query_id,
        query_start_time,
        table_database,
        table_schema,
        table_name
    from {{ ref('int_snowflake__query_table_access') }}
),

matched_queries as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.statement_type,
        qh.execution_time_ms,
        qh.partitions_scanned,
        qh.partitions_total,
        qh.bytes_scanned,
        qh.bytes_spilled_local,
        qh.bytes_spilled_remote
    from query_history as qh
    inner join query_table_access as qta
        on qh.query_id = qta.query_id
        and cast(qh.query_start_time as date) = cast(qta.query_start_time as date)
    inner join candidate_tables as ct
        on upper(qta.table_database) = upper(ct.table_database)
        and upper(qta.table_schema) = upper(ct.table_schema)
        and upper(qta.table_name) = upper(ct.table_name)
)
{% else %}
matched_queries as (
    select
        ct.platform,
        qh.stats_date,
        ct.table_database,
        ct.table_schema,
        ct.table_name,
        qh.statement_type,
        qh.execution_time_ms,
        qh.partitions_scanned,
        qh.partitions_total,
        qh.bytes_scanned,
        qh.bytes_spilled_local,
        qh.bytes_spilled_remote
    from query_history as qh
    inner join candidate_tables as ct
        on qh.query_text ilike '%' || ct.table_name || '%'
)
{% endif %}

select
    md5(
        coalesce(platform, '') || '|' ||
        coalesce(to_varchar(stats_date), '') || '|' ||
        coalesce(table_database, '') || '|' ||
        coalesce(table_schema, '') || '|' ||
        coalesce(table_name, '')
    ) as table_query_stats_daily_key,
    platform,
    stats_date,
    table_database,
    table_schema,
    table_name,
    count(*) as total_query_count,
    count(case when statement_type = 'SELECT' then 1 end) as select_count,
    count(case when statement_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE') then 1 end) as dml_count,
    sum(case when statement_type = 'SELECT' then coalesce(execution_time_ms, 0) else 0 end) as select_execution_time_ms_sum,
    sum(case when statement_type = 'SELECT' then coalesce(partitions_scanned, 0) else 0 end) as select_partitions_scanned_sum,
    sum(case when statement_type = 'SELECT' then coalesce(partitions_total, 0) else 0 end) as select_partitions_total_sum,
    sum(coalesce(bytes_scanned, 0)) as bytes_scanned_sum,
    sum(coalesce(bytes_spilled_local, 0)) as bytes_spilled_local_sum,
    sum(coalesce(bytes_spilled_remote, 0)) as bytes_spilled_remote_sum
from matched_queries
group by 1, 2, 3, 4, 5, 6
