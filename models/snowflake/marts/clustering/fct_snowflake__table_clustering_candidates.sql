{#--
  Historical snapshot of clustering candidates based on Snowflake metadata and query history.
  This model ports the logic from the find_table_clustering_candidates macro into SQL.
--#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='clustering_candidates_snapshot_key',
    post_hook=[
      "{{ refresh_column_cardinality() }}",
      "{{ extract_operator_evidence() }}"
    ]
  )
}}

{% set lookback_days = var('clustering_candidates_lookback_days', 7) %}
{% set min_size_gb = var('clustering_candidates_min_size_gb', 100) %}
{% set dbt_project_only = var('clustering_candidates_dbt_project_only', true) %}
{% set target_databases = var('clustering_candidates_target_databases', []) %}
{% set target_schemas = var('clustering_candidates_target_schemas', []) %}

with
large_tables as (
    select
        ti.database_name,
        ti.schema_name,
        ti.table_name,
        ti.active_bytes as size_bytes,
        ti.size_gb,
        ti.row_count,
        ti.is_already_clustered,
        ti.clustering_key,
        ti.approx_micropartitions,
        ti.normalized_table_type as table_type
    from {{ ref('int_snowflake__table_inventory') }} as ti
    where ti.size_gb >= {{ min_size_gb }}
        {% if target_databases and target_databases | length > 0 %}
            and upper(ti.database_name) in (
                {% for db in target_databases %}
                    '{{ db | upper }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endif %}
        {% if target_schemas and target_schemas | length > 0 %}
            and upper(ti.schema_name) in (
                {% for sc in target_schemas %}
                    '{{ sc | upper }}'{% if not loop.last %}, {% endif %}
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
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_execution_time_ms_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_execution_time_ms,
        -- Per-table pruning stats from TABLE_QUERY_PRUNING_HISTORY
        coalesce(sum(tqs.pruning_partitions_scanned_sum), 0) as pruning_partitions_scanned,
        coalesce(sum(tqs.pruning_partitions_pruned_sum), 0) as pruning_partitions_pruned,
        -- Derived scan ratio: scanned / (scanned + pruned). Null when no pruning data.
        iff(
            coalesce(sum(tqs.pruning_partitions_scanned_sum), 0) + coalesce(sum(tqs.pruning_partitions_pruned_sum), 0) > 0,
            sum(tqs.pruning_partitions_scanned_sum)::float
                / (sum(tqs.pruning_partitions_scanned_sum) + sum(tqs.pruning_partitions_pruned_sum)),
            null
        ) as scan_ratio,
        -- Legacy query-level partition stats (retained for other DAGs)
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_partitions_scanned_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_partitions_scanned,
        iff(
            coalesce(sum(tqs.select_count), 0) > 0,
            coalesce(sum(tqs.select_partitions_total_sum), 0) / nullif(sum(tqs.select_count), 0),
            0
        ) as avg_partitions_total
    from large_tables as lt
    left join {{ ref('int_snowflake__table_query_stats_daily') }} as tqs
        on lt.database_name = tqs.table_database
        and lt.schema_name = tqs.table_schema
        and lt.table_name = tqs.table_name
        and tqs.stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by lt.database_name, lt.schema_name, lt.table_name
),

scored as (
    select
        lt.database_name,
        lt.schema_name,
        lt.table_name,
        upper(lt.database_name) || '.' || upper(lt.schema_name) || '.' || upper(lt.table_name) as table_fqn,
        coalesce(dm.dbt_model, rh.node_id) as dbt_model,
        lt.table_type,
        lt.is_already_clustered,
        lt.clustering_key,
        coalesce(tqs.select_count, 0) as select_count,
        coalesce(tqs.dml_count, 0) as dml_count,
        coalesce(tqs.avg_execution_time_ms, 0) as avg_execution_time_ms,
        -- Per-table scan ratio from TABLE_QUERY_PRUNING_HISTORY (0.5 fallback)
        coalesce(tqs.scan_ratio, 0.5) as scan_ratio,
        coalesce(tqs.pruning_partitions_scanned, 0) as pruning_partitions_scanned,
        coalesce(tqs.pruning_partitions_pruned, 0) as pruning_partitions_pruned,
        -- Data availability flag
        (coalesce(tqs.pruning_partitions_scanned, 0) + coalesce(tqs.pruning_partitions_pruned, 0)) > 0 as has_pruning_data,
        -- Legacy query-level stats (retained for display)
        coalesce(tqs.avg_partitions_scanned, 0) as avg_partitions_scanned,
        coalesce(tqs.avg_partitions_total, 0) as avg_partitions_total,
        lt.size_gb,
        coalesce(lt.row_count, 0) as row_count,
        coalesce(
            nullif(coalesce(tqs.pruning_partitions_scanned, 0) + coalesce(tqs.pruning_partitions_pruned, 0), 0),
            nullif(coalesce(tqs.avg_partitions_total, 0), 0),
            lt.approx_micropartitions
        ) as estimated_micropartitions,
        case
            when (coalesce(tqs.pruning_partitions_scanned, 0) + coalesce(tqs.pruning_partitions_pruned, 0)) > 0
                then 'pruning_history'
            when coalesce(tqs.avg_partitions_total, 0) > 0
                then 'query_history'
            else 'storage_approximation'
        end as micropartition_source
    from large_tables as lt
    left join table_query_stats as tqs
        on lt.database_name = tqs.database_name
        and lt.schema_name = tqs.schema_name
        and lt.table_name = tqs.table_name
    left join {{ ref('int_dbt__relations') }} as dm
        on lt.database_name = dm.database_name
        and lt.schema_name = dm.schema_name
        and lt.table_name = dm.table_name
    left join (
        select table_fqn, max(node_id) as node_id
        from {{ ref('int_snowflake__dbt_relation_history') }}
        group by table_fqn
    ) as rh
        on rh.table_fqn = upper(lt.database_name || '.' || lt.schema_name || '.' || lt.table_name)
),

consumption_evidence as (
    -- Per-table pruning metrics from operator evidence, split by workload class
    select
        oe.table_fqn,
        -- Consumption queries (SELECT without dbt metadata)
        sum(case when wc.workload_class = 'consumption' then oe.partitions_scanned else 0 end) as consumption_partitions_scanned,
        sum(case when wc.workload_class = 'consumption' then oe.partitions_total else 0 end) as consumption_partitions_total,
        count(distinct case when wc.workload_class = 'consumption' then oe.query_id end) as consumption_query_count,
        count(distinct case when wc.workload_class = 'consumption' then oe.query_parameterized_hash end) as consumption_query_shape_count,
        -- Transformation queries (dbt builds)
        sum(case when wc.workload_class = 'dbt_model_build' then oe.partitions_scanned else 0 end) as transformation_partitions_scanned,
        sum(case when wc.workload_class = 'dbt_model_build' then oe.partitions_total else 0 end) as transformation_partitions_total,
        count(distinct case when wc.workload_class = 'dbt_model_build' then oe.query_id end) as transformation_query_count
    from {{ ref('int_snowflake__query_operator_evidence') }} as oe
    inner join {{ ref('int_snowflake__query_workload_class') }} as wc
        on oe.query_id = wc.query_id
    where oe.operator_type = 'TableScan'
      and oe.access_date >= dateadd(day, -{{ lookback_days }}, current_date())
    group by oe.table_fqn
),

final as (
    select
        -- snapshot metadata
        md5(
            to_varchar(current_date()) || '|' || coalesce(table_fqn, '')
        ) as clustering_candidates_snapshot_key,
        current_date() as snapshot_date,
        current_timestamp() as analyzed_at,
        -- table identity
        database_name,
        schema_name,
        table_name,
        table_fqn,
        dbt_model,
        table_type,
        -- recommendation (V3 scoring — uses per-table scan_ratio from TABLE_QUERY_PRUNING_HISTORY)
        (
            case
                when select_count > 0 then
                    (select_count * (avg_execution_time_ms / 1000.0))
                    * scan_ratio
                    * (1 + log(2, (select_count / iff(dml_count = 0, 1, dml_count)) + 1))
                else 0
            end
        ) as score,
        case
            when
                select_count > 0
                and (select_count / iff(dml_count = 0, 1, dml_count)) > 1
                and size_gb >= {{ min_size_gb }}
                and scan_ratio > 0.5
            then true
            else false
        end as is_candidate,
        -- Impact tier (replaces boolean-only output)
        case
            when select_count = 0 then 'No read activity'
            when (select_count / iff(dml_count = 0, 1, dml_count)) <= 1 then 'Write-heavy'
            when scan_ratio >= 0.5 and (
                (select_count * (avg_execution_time_ms / 1000.0)) * scan_ratio
                * (1 + log(2, (select_count / iff(dml_count = 0, 1, dml_count)) + 1))
            ) >= 1000 then 'High impact'
            when scan_ratio >= 0.5 then 'Moderate impact'
            when scan_ratio >= 0.2 then 'Low impact'
            else 'Healthy'
        end as recommendation_tier,
        case
            when select_count > 0
                and (select_count / iff(dml_count = 0, 1, dml_count)) > 1
                and size_gb >= {{ min_size_gb }}
                and scan_ratio > 0.5
            then
                'Queries scan '
                || round(scan_ratio * 100, 0)
                || '% of ' || estimated_micropartitions
                || ' micropartitions on average. '
                || select_count || ' reads over the lookback window at '
                || round(avg_execution_time_ms / 1000, 1) || 's average. '
                || 'Read/write ratio: ' || round(select_count / (dml_count + 1), 1) || ':1. '
                || 'Clustering on frequently-filtered columns would reduce scan overhead.'
            else
                case
                    when select_count = 0
                        then 'No read activity in lookback window.'
                    when (select_count / iff(dml_count = 0, 1, dml_count)) <= 1
                        then 'Write-heavy workload — clustering ROI unclear due to reclustering churn.'
                    when scan_ratio <= 0.5
                        then 'Pruning already effective (scanning ' || round(scan_ratio * 100, 0) || '% of partitions on average).'
                    else 'Below size threshold or insufficient data for recommendation.'
                end
        end as recommendation_reason,
        -- clustering state
        is_already_clustered,
        clustering_key,
        -- size & structure
        size_gb as table_size_gb,
        row_count as total_rows,
        estimated_micropartitions,
        micropartition_source,
        case
            when estimated_micropartitions > 0 then round(row_count / estimated_micropartitions, 2)
            else 0
        end as avg_rows_per_micropartition,
        has_pruning_data,
        round(scan_ratio * 100, 2) as scan_ratio_pct,
        avg_partitions_scanned,
        -- query activity
        select_count,
        dml_count,
        round(select_count / (dml_count + 1), 1) as query_to_dml_ratio,
        round(avg_execution_time_ms / 1000, 2) as avg_query_duration_s,
        -- Consumption-specific pruning from operator evidence
        coalesce(ce.consumption_query_count, 0) as consumption_query_count,
        coalesce(ce.consumption_query_shape_count, 0) as consumption_query_shape_count,
        case
            when coalesce(ce.consumption_partitions_total, 0) > 0
                then round(ce.consumption_partitions_scanned::float / ce.consumption_partitions_total * 100, 2)
            else null
        end as consumption_scan_ratio_pct,
        case
            when coalesce(ce.transformation_partitions_total, 0) > 0
                then round(ce.transformation_partitions_scanned::float / ce.transformation_partitions_total * 100, 2)
            else null
        end as transformation_scan_ratio_pct,
        coalesce(ce.transformation_query_count, 0) as transformation_query_count,
        -- Recommendation status (consumption-driven)
        case
            when coalesce(ce.consumption_query_count, 0) = 0 then 'insufficient_evidence'
            when (ce.consumption_partitions_scanned::float / nullif(ce.consumption_partitions_total, 0)) > 0.5
                and not is_already_clustered then 'evaluate_clustering'
            when (ce.consumption_partitions_scanned::float / nullif(ce.consumption_partitions_total, 0)) > 0.5
                and is_already_clustered then 'evaluate_key_alignment'
            when (ce.consumption_partitions_scanned::float / nullif(ce.consumption_partitions_total, 0)) <= 0.5
                and coalesce(ce.transformation_partitions_total, 0) > 0
                and (ce.transformation_partitions_scanned::float / nullif(ce.transformation_partitions_total, 0)) > 0.8
                then 'optimize_transformation'
            when (ce.consumption_partitions_scanned::float / nullif(ce.consumption_partitions_total, 0)) <= 0.5
                and is_already_clustered then 'keep_current_key'
            when (ce.consumption_partitions_scanned::float / nullif(ce.consumption_partitions_total, 0)) <= 0.5
                then 'healthy'
            else 'investigate'
        end as recommendation_status
    from scored
    left join consumption_evidence as ce
        on ce.table_fqn = upper(scored.database_name || '.' || scored.schema_name || '.' || scored.table_name)
    where
        {% if dbt_project_only %}
            dbt_model is not null
        {% else %}
            1 = 1
        {% endif %}
)

select
    -- snapshot metadata
    clustering_candidates_snapshot_key,
    snapshot_date,
    analyzed_at,
    -- table identity
    database_name,
    schema_name,
    table_name,
    table_fqn,
    dbt_model,
    table_type,
    -- recommendation
    is_candidate,
    recommendation_tier,
    score,
    recommendation_reason,
    -- clustering state
    is_already_clustered,
    clustering_key,
    -- size & structure
    table_size_gb,
    total_rows,
    estimated_micropartitions,
    micropartition_source,
    avg_rows_per_micropartition,
    has_pruning_data,
    scan_ratio_pct,
    avg_partitions_scanned,
    -- query activity
    select_count,
    dml_count,
    query_to_dml_ratio,
    avg_query_duration_s,
    -- consumption evidence
    consumption_query_count,
    consumption_query_shape_count,
    consumption_scan_ratio_pct,
    transformation_scan_ratio_pct,
    transformation_query_count,
    recommendation_status
from final
{% if is_incremental() %}
where snapshot_date >= (
    select coalesce(max(snapshot_date), '1970-01-01'::date)
    from {{ this }}
)
{% endif %}
