{{
  config(
    materialized='incremental',
    unique_key=['query_id', 'table_database', 'table_schema', 'table_name'],
    incremental_strategy='merge',
    cluster_by=['to_date(query_start_time)'],
  )
}}

{#--
  Flattens access_history into one row per (query_id, table) for object-level attribution.
  Replaces query_text ILIKE matching with exact engine-reported base_objects_accessed and
  objects_modified. Used by table_query_stats_daily and other recommendation marts.

  Enterprise+ only (requires ACCESS_HISTORY view).
  Grain: one row per (query_id, table_database, table_schema, table_name)
--#}

with access_base as (
    select
        query_id,
        query_start_time,
        base.value as obj,
        'read' as access_type
    from {{ ref('stg_snowflake__access_history') }}
    left join lateral flatten(input => base_objects_accessed) as base
    where base_objects_accessed is not null
      {% if is_incremental() %}
        and query_start_time >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(query_start_time)) from {{ this }})
      {% endif %}
),

access_modified as (
    select
        query_id,
        query_start_time,
        mod.value as obj,
        'write' as access_type
    from {{ ref('stg_snowflake__access_history') }}
    left join lateral flatten(input => objects_modified) as mod
    where objects_modified is not null
      {% if is_incremental() %}
        and query_start_time >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(query_start_time)) from {{ this }})
      {% endif %}
),

unioned as (
    select query_id, query_start_time, obj, access_type from access_base
    union all
    select query_id, query_start_time, obj, access_type from access_modified
),

parsed as (
    select
        query_id,
        query_start_time,
        access_type,
        upper(trim(replace(obj:objectName::string, '"', ''))) as object_name_clean
    from unioned
    where upper(trim(coalesce(obj:objectDomain::string, ''))) in ('TABLE', 'MATERIALIZED VIEW')
        and obj:objectName::string is not null
        and trim(obj:objectName::string) != ''
),

split as (
    select
        query_id,
        query_start_time,
        access_type,
        split_part(object_name_clean, '.', 1) as table_database,
        split_part(object_name_clean, '.', 2) as table_schema,
        split_part(object_name_clean, '.', 3) as table_name
    from parsed
    where split_part(object_name_clean, '.', 3) != ''
)

select distinct
    query_id,
    query_start_time,
    table_database,
    table_schema,
    table_name
from split
