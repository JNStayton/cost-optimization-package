{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='column_query_stats_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'bigquery' and var('use_query_text_attribution', true))
  )
}}

{#--
  Daily column-level query access counts per BigQuery table, aggregated from
  int_bigquery__column_query_access. Mirrors int_snowflake__column_query_stats.

  Initial backfill: 30 days. Incremental runs: yesterday onwards (1-day buffer
  for late-arriving JOBS_BY_PROJECT data, matching the Snowflake pattern).
--#}

{% set initial_lookback_days = var('column_query_stats_initial_lookback_days', 30) %}

with column_access as (
    select
        query_id,
        query_start_time,
        table_fqn,
        table_database,
        table_schema,
        table_name,
        column_name
    from {{ ref('int_bigquery__column_query_access') }}
    {% if is_incremental() %}
        where cast(query_start_time as date) >= date_sub(
            (select coalesce(max(access_date), date('1970-01-01')) from {{ this }}),
            interval 1 day
        )
    {% else %}
        where query_start_time >= timestamp_sub(current_timestamp(), interval {{ initial_lookback_days }} day)
    {% endif %}
)

select
    to_hex(md5(
        cast(cast(query_start_time as date) as string) || '|' ||
        coalesce(table_fqn, '') || '|' ||
        coalesce(column_name, '')
    )) as column_query_stats_daily_key,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    cast(query_start_time as date) as access_date,
    count(distinct query_id) as query_count
from column_access
group by
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date
