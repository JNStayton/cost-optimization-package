{{
  config(
    materialized='view',
    enabled=(target.type == 'snowflake' and var('use_access_history_attribution', true))
  )
}}

{#--
  One row per (query_id, table_fqn, column_name) from Snowflake ACCESS_HISTORY.
  Extends int_snowflake__query_table_access by flattening one level deeper —
  from object-level down to the columns[] array within each accessed object.

  This gives exact engine-reported column-level read attribution without any
  query_text parsing. Used by int_snowflake__column_query_stats to produce
  per-column usage counts for clustering key scoring.

  Enterprise+ only (requires ACCESS_HISTORY view). Disabled when
  use_access_history_attribution = false.
--#}

with objects_flattened as (
    select
        query_id,
        query_start_time,
        obj.value as obj
    from {{ ref('stg_snowflake__access_history') }}
    join lateral flatten(input => base_objects_accessed) as obj
    where base_objects_accessed is not null
        and upper(trim(coalesce(obj.value:objectDomain::string, ''))) in ('TABLE', 'MATERIALIZED VIEW')
        and obj.value:columns is not null
),

columns_flattened as (
    select
        query_id,
        query_start_time,
        upper(trim(replace(obj:objectName::string, '"', ''))) as object_name_clean,
        upper(trim(col.value:columnName::string)) as column_name
    from objects_flattened
    join lateral flatten(input => obj:columns) as col
    where col.value:columnName::string is not null
        and trim(col.value:columnName::string) != ''
)

select distinct
    query_id,
    query_start_time,
    object_name_clean as table_fqn,
    split_part(object_name_clean, '.', 1) as table_database,
    split_part(object_name_clean, '.', 2) as table_schema,
    split_part(object_name_clean, '.', 3) as table_name,
    column_name
from columns_flattened
where split_part(object_name_clean, '.', 3) != ''
