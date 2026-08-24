{% if var('use_mock_data', false) %}

select
    catalog_name as database_name,
    schema_name,
    table_name,
    table_id,
    case
        when table_type = 'MANAGED_TABLE' then 'BASE TABLE'
        when table_type = 'EXTERNAL_TABLE' then 'EXTERNAL TABLE'
        else table_type
    end as table_type,
    row_count,
    case
        when partition_columns is not null and size(partition_columns) > 0
        then array_join(partition_columns, ', ')
        else null
    end as clustering_key,
    cast(false as boolean) as is_transient,
    case when deleted_at is not null then true else false end as is_deleted,
    'databricks' as platform
from {{ ref('stg_databricks__table_info') }}

{% else %}

-- system.storage.table_metrics_history is empty unless Predictive Optimization
-- is enabled, so table identity comes from information_schema (always populated)
-- and storage metrics are enriched in on a best-effort basis.
with info_schema as (
    select * from {{ ref('stg_databricks__tables') }}
),

storage_metrics as (
    select * from {{ ref('stg_databricks__table_info') }}
)

select
    info_schema.table_catalog as database_name,
    info_schema.table_schema as schema_name,
    info_schema.table_name,
    storage_metrics.table_id,
    case
        when info_schema.table_type = 'MANAGED' then 'BASE TABLE'
        when info_schema.table_type = 'EXTERNAL' then 'EXTERNAL TABLE'
        else info_schema.table_type
    end as table_type,
    storage_metrics.row_count,
    case
        when storage_metrics.partition_columns is not null and size(storage_metrics.partition_columns) > 0
        then array_join(storage_metrics.partition_columns, ', ')
        else null
    end as clustering_key,
    cast(false as boolean) as is_transient,
    case when storage_metrics.deleted_at is not null then true else false end as is_deleted,
    'databricks' as platform
from info_schema
left join storage_metrics
    on info_schema.table_catalog = storage_metrics.catalog_name
    and info_schema.table_schema = storage_metrics.schema_name
    and info_schema.table_name = storage_metrics.table_name

{% endif %}
