{#--
  BigQuery clustering candidates.

  Scores tables using BigQuery's primary cost signals:
    - total_bytes_billed (how much each query costs)
    - total_slot_ms (compute intensity)
    - total_partitions (data density proxy for partition_density_multiplier)
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_candidates_snapshot_key',
    enabled=(target.type == 'bigquery'),
    post_hook="{{ refresh_bigquery_column_cardinality() }}"
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('clustering_candidates_min_size_gb', 1000) %}
{% set dbt_project_only = var('clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('clustering_candidates_target_databases', []) %}
{% set target_schemas = var('clustering_candidates_target_schemas', []) %}

with
-- No row limit applied here. Unlike the Snowflake sibling (limit 100), BigQuery's
-- distributed execution handles large scans efficiently. Override clustering_candidates_min_size_gb
-- to narrow the candidate set if needed.
large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.approx_micropartitions,
        ti.normalized_table_type as table_type
    from {{ ref('int_bigquery__table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
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

table_query_stats as (
    select
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        coalesce(sum(tqs.select_count), 0) as select_count,
        coalesce(sum(tqs.dml_count), 0) as dml_count,
        -- avg_slot_ms: average total_slot_ms per SELECT query (compute intensity signal)
        if(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_execution_time_ms_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_slot_ms,
        -- avg_bytes_billed: average total_bytes_billed per SELECT query (primary cost signal)
        if(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_bytes_billed_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_bytes_billed
    from large_tables as lt
    left join {{ ref('int_bigquery__table_query_stats_daily') }} as tqs
        on lt.database_name = tqs.table_database
        and lt.schema_name = tqs.table_schema
        and lt.table_name = tqs.table_name
        and tqs.stats_date >= date_sub(current_date(), interval {{ lookback_days }} day)
    group by 1, 2, 3
),

scored as (
    select
        current_timestamp() as analyzed_at,
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        lt.database_name || '.' || lt.schema_name || '.' || lt.table_name as table_fqn,
        dm.dbt_model,
        lt.table_type,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_slot_ms, 0) as avg_slot_ms,
        coalesce(tqs.avg_bytes_billed, 0) as avg_bytes_billed,
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        -- approx_micropartitions = total_partitions for BigQuery
        lt.approx_micropartitions as total_partitions
    from large_tables as lt
    left join table_query_stats as tqs
        on lt.database_name = tqs.database_name
        and lt.schema_name = tqs.schema_name
        and lt.table_name = tqs.table_name
    left join {{ ref('int_dbt__relations') }} as dm
        on lt.database_name = dm.database_name
        and lt.schema_name = dm.schema_name
        and lt.table_name = dm.table_name
),

final as (
    select
        analyzed_at,
        current_date() as snapshot_date,
        to_hex(md5(
            cast(current_date() as string) || '|' || coalesce(table_fqn, '')
        )) as clustering_candidates_snapshot_key,
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        -- Score: query volume × avg GB billed per query × read-heavy bonus × partition density
        -- Higher = more bytes billed per query, more frequent reads, more partitions per row
        (
            case
                when select_count > 0 then
                    -- avg GB billed per query (primary BigQuery cost signal)
                    (select_count * (avg_bytes_billed / power(1024, 3)))
                    -- read-heavy bonus: tables queried far more than written benefit most
                    + ((cast(select_count as float64) / (dml_count + 1)) * 10)
                else 0
            end
        )
        * (
            -- partition density multiplier: many partitions relative to row count
            -- suggests fragmented data that clustering can consolidate
            case
                when row_count > 0 and total_partitions > 0
                    then greatest(cast(total_partitions as float64) / row_count * 1000, 1)
                else 1
            end
        ) as score,
        case
            when
                select_count > 0
                and (cast(select_count as float64) / (dml_count + 1)) > 1
                and size_gb >= {{ min_size_gb }}
            then true
            else false
        end as is_candidate,
        size_gb as table_size_gb,
        row_count as total_rows,
        total_partitions as current_partitions,
        round(avg_bytes_billed / power(1024, 3), 4) as avg_gb_billed_per_query,
        select_count,
        dml_count,
        round(cast(select_count as float64) / (dml_count + 1), 1) as query_to_dml_ratio,
        round(avg_slot_ms / 1000, 2) as avg_slot_seconds
    from scored
    -- dbt_project_only filter is deferred to this final CTE (consistent with Snowflake sibling).
    -- Tables without a dbt_model join still flow through table_query_stats aggregation; they
    -- are filtered out here rather than in large_tables.
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select
    analyzed_at,
    snapshot_date,
    clustering_candidates_snapshot_key,
    database_name,
    schema_name,
    table_name,
    table_fqn,
    dbt_model,
    table_type,
    score,
    is_candidate,
    table_size_gb,
    total_rows,
    current_partitions,
    avg_gb_billed_per_query,
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_slot_seconds
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), date('1970-01-01'))
    from {{ this }}
)
{% endif %}
