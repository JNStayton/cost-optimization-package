{{
  config(
    materialized='incremental',
    unique_key=['query_id', 'table_fqn'],
    incremental_strategy='merge',
    cluster_by=['to_date(query_start_time)'],
  )
}}

{#--
  Flattens ACCESS_HISTORY.direct_objects_accessed to provide exact engine-reported
  query-to-object attribution at the view/table level.

  Unlike base_objects_accessed (which resolves through views to the underlying tables),
  direct_objects_accessed preserves the actual objects referenced in the query — including
  views. This is critical for table materialization analysis where we need to know
  how many queries hit a given view.

  Enterprise+ only (requires ACCESS_HISTORY).
  Grain: one row per (query_id, table_fqn)
--#}

with direct_access as (
    select
        query_id,
        query_start_time,
        f.value:objectDomain::string as object_domain,
        upper(trim(replace(f.value:objectName::string, '"', ''))) as object_name_raw
    from {{ ref('stg_snowflake__access_history') }}
    join lateral flatten(input => direct_objects_accessed) as f
    where direct_objects_accessed is not null
      and f.value:objectDomain::string in ('View', 'Table', 'Materialized View')
      and f.value:objectName::string is not null
      and trim(f.value:objectName::string) != ''
      {% if is_incremental() %}
        and query_start_time >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(query_start_time)) from {{ this }})
      {% endif %}
)

select distinct
    query_id,
    query_start_time,
    object_domain,
    object_name_raw as table_fqn,
    split_part(object_name_raw, '.', 1) as table_database,
    split_part(object_name_raw, '.', 2) as table_schema,
    split_part(object_name_raw, '.', 3) as table_name
from direct_access
where split_part(object_name_raw, '.', 3) != ''
