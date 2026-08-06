{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='column_query_stats_daily_key',
    cluster_by=['access_date'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Daily column-level query access counts per table, derived from ACCESS_HISTORY.
  One row per (access_date, table_fqn, column_name).

  query_count: total queries that accessed the column (any context).

  This model tracks raw access frequency only. Filter/join usage context
  comes from int_snowflake__query_operator_columns (GET_QUERY_OPERATOR_STATS).

  Used by:
  - fct_snowflake__clustering_key_candidates (access frequency signal)
  - refresh_column_cardinality (pre-filter which columns to profile)

  Enterprise+ only. Disabled when snowflake_enterprise_edition = false.
--#}

{% set overlap_days = var('incremental_overlap_days', 31) %}

with column_access as (
    select
        ca.table_fqn,
        ca.table_database,
        ca.table_schema,
        ca.table_name,
        ca.column_name,
        ca.query_id,
        cast(ca.query_start_time as date) as access_date
    from {{ ref('int_snowflake__column_query_access') }} as ca
    {% if is_incremental() %}
        where ca.query_start_time >= dateadd(
            day,
            -1,
            (
                select coalesce(max(access_date), '1970-01-01'::date)
                from {{ this }}
            )
        )
    {% else %}
        where ca.query_start_time >= dateadd(day, -{{ overlap_days }}, current_timestamp())
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
group by
    column_query_stats_daily_key,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date
