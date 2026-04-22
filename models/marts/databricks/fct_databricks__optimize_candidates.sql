{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='optimize_candidates_snapshot_key',
    enabled=(target.type == 'databricks')
) }}

{% set min_file_count = var('optimize_candidates_min_file_count', 50) %}
{% set max_avg_file_size_mb = var('optimize_candidates_max_avg_file_size_mb', 128) %}
{% set min_size_gb = var('optimize_candidates_min_size_gb', 0.1) %}
{% set dbt_project_only = var('optimize_candidates_dbt_project_only', true) %}
{% set target_databases = var('optimize_candidates_target_databases', []) %}
{% set target_schemas = var('optimize_candidates_target_schemas', []) %}

with fragmented_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.table_fqn,
        ti.normalized_table_type as table_type,
        ti.size_gb,
        ti.file_count,
        ti.active_bytes,
        case
            when ti.file_count > 0
            then (ti.active_bytes / 1024.0 / 1024.0) / ti.file_count
            else null
        end as avg_file_size_mb,
        ti.is_already_clustered
    from {{ ref('int_table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        and ti.file_count >= {{ min_file_count }}
        and ti.file_count is not null
        {% if target_databases and target_databases | length > 0 %}
            and ti.database_name in (
                {% for db in target_databases %}
                    '{{ db }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and ti.schema_name in (
                {% for sc in target_schemas %}
                    '{{ sc }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
),

with_po_status as (
    select
        ft.*,
        coalesce(si.predictive_optimization_enabled, false) as predictive_optimization_enabled
    from fragmented_tables as ft
    left join {{ ref('stg_databricks__table_info') }} as si
        on ft.database_name = si.catalog_name
        and ft.schema_name = si.schema_name
        and ft.table_name = si.table_name
),

with_dbt_model as (
    select
        wp.*,
        dm.dbt_model
    from with_po_status as wp
    left join {{ ref('int_dbt__relations') }} as dm
        on wp.database_name = dm.database_name
        and wp.schema_name = dm.schema_name
        and wp.table_name = dm.table_name
),

final as (
    select
        current_date() as snapshot_date,
        md5(
            cast(current_date() as string) || '|' || coalesce(table_fqn, '')
        ) as optimize_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        size_gb as table_size_gb,
        file_count as current_file_count,
        avg_file_size_mb,
        is_already_clustered,
        predictive_optimization_enabled,
        case
            when not predictive_optimization_enabled and avg_file_size_mb < {{ max_avg_file_size_mb }}
            then 'RUN OPTIMIZE'
            when not predictive_optimization_enabled and is_already_clustered
            then 'ENABLE PREDICTIVE OPTIMIZATION'
            else 'ENABLE PREDICTIVE OPTIMIZATION'
        end as recommended_action,
        round(
            file_count * ({{ max_avg_file_size_mb }}.0 / greatest(avg_file_size_mb, 1)),
            1
        ) as score,
        case
            when not predictive_optimization_enabled
                and avg_file_size_mb < {{ max_avg_file_size_mb }}
            then true
            else false
        end as is_candidate
    from with_dbt_model
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select * from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
