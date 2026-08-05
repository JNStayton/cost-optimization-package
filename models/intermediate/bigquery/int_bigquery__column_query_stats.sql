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

  Initial backfill and incremental runs are both bounded by the upstream
  int_bigquery__column_query_access window (clustering_candidates_lookback_days,
  default 7) — that view scans JOBS_BY_PROJECT via query-text matching, which is
  too costly to run unbounded, unlike Snowflake's ACCESS_HISTORY-based path.
  column_query_stats_initial_lookback_days can narrow the first-run window
  further but can never see data older than the upstream cap.
--#}

{% set initial_lookback_days = var('column_query_stats_initial_lookback_days', var('clustering_candidates_lookback_days', 7)) %}

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
),

column_access_dated as (
    select
        query_id,
        table_fqn,
        table_database,
        table_schema,
        table_name,
        column_name,
        cast(query_start_time as date) as access_date
    from column_access
)

select
    to_hex(md5(
        cast(access_date as string) || '|' ||
        coalesce(table_fqn, '') || '|' ||
        coalesce(column_name, '')
    )) as column_query_stats_daily_key,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date,
    count(distinct query_id) as query_count
from column_access_dated
group by
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date
