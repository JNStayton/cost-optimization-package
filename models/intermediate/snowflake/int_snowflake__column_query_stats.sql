{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='column_query_stats_daily_key',
    on_schema_change='append_new_columns',
    enabled=(target.type == 'snowflake' and var('use_access_history_attribution', true))
  )
}}

{#--
  Daily column-level query access counts per table, derived from ACCESS_HISTORY.
  One row per (access_date, table_fqn, column_name).

  Used by fct_snowflake__clustering_key_candidates and the refresh_column_cardinality
  macro to pre-filter which columns are worth running APPROX_COUNT_DISTINCT against —
  only columns that have actually appeared in query access are candidates for
  cardinality scanning, keeping the macro compute cost contained.

  Enterprise+ only. Disabled when use_access_history_attribution = false.
  Standard edition users receive cardinality scoring only (no usage signal).

  Initial lookback: 30 days. Override with vars.column_query_stats_initial_lookback_days.
--#}

{% set initial_lookback_days = var('column_query_stats_initial_lookback_days', 30) %}

with column_access as (
    select
        table_fqn,
        table_database,
        table_schema,
        table_name,
        column_name,
        query_id,
        cast(query_start_time as date) as access_date
    from {{ ref('int_snowflake__column_query_access') }}
    {% if is_incremental() %}
        where query_start_time >= dateadd(
            day,
            -1,
            (
                select coalesce(max(access_date), '1970-01-01'::date)
                from {{ this }}
            )
        )
    {% else %}
        where query_start_time >= dateadd(day, -{{ initial_lookback_days }}, current_timestamp())
    {% endif %}
)

select
    md5(
        coalesce(to_varchar(access_date), '') || '|' ||
        coalesce(table_fqn, '') || '|' ||
        coalesce(column_name, '')
    ) as column_query_stats_daily_key,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date,
    count(distinct query_id) as query_count
from column_access
group by column_query_stats_daily_key, table_fqn, table_database, table_schema, table_name, column_name, access_date
