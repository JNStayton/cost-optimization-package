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
  Daily column-level query access counts per table, derived from ACCESS_HISTORY
  joined with QUERY_HISTORY for query text classification.
  One row per (access_date, table_fqn, column_name).

  query_count:       total queries that accessed the column (any context).
  where_query_count: queries where the column appeared in a WHERE clause.
  join_query_count:  queries where the column appeared in a JOIN...ON clause.

  where_query_count and join_query_count are the primary signals used by
  fct_snowflake__clustering_key_candidates — only filter and join usage
  drives micropartition pruning benefit in Snowflake.

  Known limitation: column aliasing (SELECT col AS alias ... WHERE alias = x)
  will not be detected; the base column name is matched against query_text.

  Also used by refresh_column_cardinality to pre-filter which columns are
  worth running APPROX_COUNT_DISTINCT against (query_count, any access).

  Enterprise+ only. Disabled when use_access_history_attribution = false.
  Initial lookback: 30 days. Override with clustering_query_stats_initial_lookback_days.
--#}

{% set initial_lookback_days = var('column_query_stats_initial_lookback_days', 30) %}

with column_access as (
    select
        ca.table_fqn,
        ca.table_database,
        ca.table_schema,
        ca.table_name,
        ca.column_name,
        ca.query_id,
        cast(ca.query_start_time as date) as access_date,
        qh.query_text
    from {{ ref('int_snowflake__column_query_access') }} as ca
    left join {{ ref('int_snowflake__query_history') }} as qh
        on ca.query_id = qh.query_id
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
        where ca.query_start_time >= dateadd(day, -{{ initial_lookback_days }}, current_timestamp())
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
    count(distinct query_id) as query_count,
    count(distinct case
        when query_text ilike '%WHERE%' || column_name || '%'
            then query_id
    end) as where_query_count,
    count(distinct case
        when query_text ilike '%JOIN%'
            and query_text ilike '%ON%' || column_name || '%'
            then query_id
    end) as join_query_count
from column_access
group by
    column_query_stats_daily_key,
    table_fqn,
    table_database,
    table_schema,
    table_name,
    column_name,
    access_date
