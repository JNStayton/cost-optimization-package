{{
  config(
    materialized='view',
    enabled=(target.type == 'bigquery')
  )
}}

{#--
  BigQuery table inventory: joins table metadata with storage metrics.
  Mirrors int_snowflake__table_inventory schema for cross-platform compatibility.

  Column name notes (Snowflake convention; could be made neutral in future):
    - approx_micropartitions: Snowflake = active_bytes / 16 MB (micropartition count approximation)
                              BigQuery  = total_partitions (actual date/range partition count)
--#}

select
    t.platform,
    t.database_name,
    t.schema_name,
    t.table_name,
    t.database_name || '.' || t.schema_name || '.' || t.table_name as table_fqn,
    t.table_type,
    case
        when t.table_type = 'MATERIALIZED VIEW' then 'Materialized View'
        else 'Permanent Table'
    end as normalized_table_type,
    coalesce(s.total_rows, 0) as row_count,
    t.clustering_key,
    t.clustering_key is not null as is_already_clustered,
    false as is_transient,
    s.active_bytes,
    s.active_bytes / pow(1024, 3) as size_gb,
    -- approx_micropartitions: BigQuery uses actual partition count as the data-density proxy.
    -- Snowflake equivalent is active_bytes / (16 * 1024 * 1024).
    coalesce(s.total_partitions, 0) as approx_micropartitions
from {{ ref('int_bigquery__tables') }} as t
inner join {{ ref('int_bigquery__table_storage') }} as s
    on t.database_name = s.database_name
    and t.schema_name = s.schema_name
    and t.table_name = s.table_name
where t.table_type in ('BASE TABLE', 'MATERIALIZED VIEW')
    and not coalesce(t.is_deleted, false)
    and not coalesce(s.is_deleted, false)
