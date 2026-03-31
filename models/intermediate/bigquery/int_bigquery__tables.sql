{#--
  BigQuery table metadata. Joins stg_bigquery__table_storage to populate row_count,
  is_deleted, and clustering_key (parsed from DDL).

  Note: column names follow Snowflake convention; a future refactor could make them
  platform-neutral across all int_{platform}__tables models.
--#}
select
    t.table_catalog as database_name,
    t.table_schema as schema_name,
    t.table_name,
    cast(null as string) as table_id,
    t.table_type,
    -- total_rows from TABLE_STORAGE; INFORMATION_SCHEMA.TABLES does not expose row count
    s.total_rows as row_count,
    -- Extract clustering columns from DDL, e.g. "CLUSTER BY col1, col2\n"
    trim(regexp_extract(t.ddl, r'(?i)CLUSTER BY (.+?)(?:\n|;|$)')) as clustering_key,
    -- BigQuery has no transient table concept
    false as is_transient,
    -- deleted flag from TABLE_STORAGE (true while table is in time-travel window after deletion)
    coalesce(s.deleted, false) as is_deleted,
    'bigquery' as platform
from {{ ref('stg_bigquery__tables') }} as t
left join {{ ref('stg_bigquery__table_storage') }} as s
    on t.table_catalog = s.project_id
    and t.table_schema = s.table_schema
    and t.table_name = s.table_name
