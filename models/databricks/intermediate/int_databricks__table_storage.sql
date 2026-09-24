-- depends_on: {{ ref('int_dbt__relations') }}
-- depends_on: {{ ref('int_databricks__tables') }}
{{ config(
    pre_hook="{{ capture_storage_metrics() }}"
) }}

{% if var('use_mock_data', false) %}

select
    catalog_name as database_name,
    schema_name,
    table_name,
    data_size_bytes as active_bytes,
    cast(null as bigint) as time_travel_bytes,
    cast(null as bigint) as failsafe_bytes,
    file_count,
    case when deleted_at is not null then true else false end as is_deleted,
    'databricks' as platform
from {{ ref('stg_databricks__table_info') }}

{% else %}

-- system.storage.table_metrics_history can be empty account-wide even with
-- Predictive Optimization active (see capture_storage_metrics()).
-- The pre-hook probes dbt-tracked tables via ANALYZE TABLE COMPUTE STORAGE
-- METRICS as a fallback for any table missing from table_metrics_history.
with from_table_metrics_history as (
    select
        catalog_name as database_name,
        schema_name,
        table_name,
        data_size_bytes as active_bytes,
        cast(null as bigint) as time_travel_bytes,
        cast(null as bigint) as failsafe_bytes,
        file_count,
        case when deleted_at is not null then true else false end as is_deleted,
        'databricks' as platform
    from {{ ref('stg_databricks__table_info') }}
),

from_analyze_probe as (
    select
        database_name,
        schema_name,
        table_name,
        active_bytes,
        time_travel_bytes,
        cast(null as bigint) as failsafe_bytes,
        file_count,
        false as is_deleted,
        'databricks' as platform
    from {{ this.database }}.{{ this.schema }}.int_databricks__table_storage_metrics_probe
)

select * from from_table_metrics_history
union all
select p.* from from_analyze_probe as p
where not exists (
    select 1 from from_table_metrics_history as h
    where h.database_name = p.database_name
        and h.schema_name = p.schema_name
        and h.table_name = p.table_name
)

{% endif %}
