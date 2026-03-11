{#--
  Daily query stats per table. Attribution uses either access_history (Enterprise+)
  or query_text ILIKE (Standard). Set vars.use_access_history_attribution = false
  in dbt_project.yml for Standard edition (no ACCESS_HISTORY view).

  Scope:
    - Default: only tables that are dbt models in the current project.
    - Set vars.table_query_stats_full_account = true to scan all tables in the account.

  Initial lookback:
    - Enterprise (access_history): 30 days
    - Standard (query_text): 7 days
    - Override with vars.table_query_stats_initial_lookback_days
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

{% set use_access_history = var('use_access_history_attribution', true) %}
{% set full_account = var('table_query_stats_full_account', false) %}
{% set default_lookback = 30 if use_access_history else 7 %}
{% set initial_lookback_days = var('table_query_stats_initial_lookback_days', default_lookback) %}

with candidate_tables as (
    {% if full_account %}
    select distinct
        platform,
        database_name as table_database,
        schema_name as table_schema,
        table_name
    from {{ ref('int_snowflake__table_inventory') }}
    {% else %}
    select distinct
        ti.platform,
        ti.database_name as table_database,
        ti.schema_name as table_schema,
        ti.table_name
    from {{ ref('int_snowflake__table_inventory') }} as ti
    inner join {{ ref('int_dbt__relations') }} as dm
        on upper(ti.database_name) = upper(dm.database_name)
        and upper(ti.schema_name) = upper(dm.schema_name)
        and upper(ti.table_name) = upper(dm.table_name)
    {% endif %}
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
    {% else %}
        and query_start_time >= dateadd(day, -{{ initial_lookback_days }}, current_timestamp())
    {% endif %}
),

{% if use_access_history %}
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
