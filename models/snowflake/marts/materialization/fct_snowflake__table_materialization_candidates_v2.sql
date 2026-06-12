{{
  config(
    materialized='table',
  )
}}

{#--
  V2 of table materialization candidates.

  Keeps the existing recommendation logic intact, but makes query-text attribution
  more transparent by classifying each matched query into a single attribution tier:
    - high:   fully-qualified DATABASE.SCHEMA.OBJECT match in query_text
    - medium: schema-qualified SCHEMA.OBJECT match in query_text
    - low:    bare OBJECT name match only

  Also adds recommendation_confidence based on the quantity of corroborating
  signals rather than any single signal in isolation.

  Controlled by the following dbt variables:
    - table_materialization_lookback_days   (default 14)
    - table_materialization_min_query_count (default 10)
--#}

{% set lookback_days = var('table_materialization_lookback_days', 14) %}
{% set min_query_count = var('table_materialization_min_query_count', 10) %}

with view_candidates as (
    select
        upper(database_name) as database_name,
        upper(schema_name)   as schema_name,
        upper(table_name)    as table_name,
        upper(database_name) || '.' || upper(schema_name) || '.' || upper(table_name) as table_fqn,
        dbt_model,
        model_name,
        package_name,
        materialized
    from {{ ref('int_dbt__relations') }}
    where lower(materialized) in ('view', 'ephemeral')
),

matched_queries as (
    select
        vc.table_fqn,
        vc.database_name,
        vc.schema_name,
        vc.table_name,
        vc.dbt_model,
        vc.model_name,
        vc.package_name,
        vc.materialized,
        qh.query_id,
        qh.execution_time_ms,
        coalesce(qh.bytes_scanned, 0) as bytes_scanned,
        case
            when qh.query_text ilike '%' || vc.database_name || '.' || vc.schema_name || '.' || vc.table_name || '%'
                then 'high'
            when qh.query_text ilike '%' || vc.schema_name || '.' || vc.table_name || '%'
                then 'medium'
            when qh.query_text ilike '%' || vc.table_name || '%'
                then 'low'
        end as attribution_confidence
    from view_candidates as vc
    left join {{ ref('int_snowflake__query_history') }} as qh
        on qh.query_type = 'SELECT'
       and qh.query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
       and (
            qh.query_text ilike '%' || vc.database_name || '.' || vc.schema_name || '.' || vc.table_name || '%'
            or qh.query_text ilike '%' || vc.schema_name || '.' || vc.table_name || '%'
            or qh.query_text ilike '%' || vc.table_name || '%'
       )
),

query_stats as (
    select
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        model_name,
        package_name,
        materialized,
        count(distinct query_id)                                                  as select_count,
        avg(execution_time_ms) / 1000.0                                           as avg_query_duration_s,
        sum(bytes_scanned) / power(1024, 3)                                       as total_gb_scanned,
        count(distinct query_id) * avg(execution_time_ms) / 1000.0                as materialization_score,
        coalesce(
            (sum(bytes_scanned) / power(1024, 3)) / nullif(count(distinct query_id), 0),
            0
        )                                                                         as avg_gb_scanned_per_query,
        count(distinct case when attribution_confidence = 'high' then query_id end)   as high_confidence_query_count,
        count(distinct case when attribution_confidence = 'medium' then query_id end) as medium_confidence_query_count,
        count(distinct case when attribution_confidence = 'low' then query_id end)    as low_confidence_query_count
    from matched_queries
    group by
        table_fqn,
        database_name,
        schema_name,
        table_name,
        dbt_model,
        model_name,
        package_name,
        materialized
),

scored_stats as (
    select
        *,
        avg_query_duration_s / nullif(avg(avg_query_duration_s) over (), 0) as relative_duration_ratio,
        case
            when high_confidence_query_count > 0 then 'high'
            when medium_confidence_query_count > 0 then 'medium'
            when low_confidence_query_count > 0 then 'low'
            else 'low'
        end as attribution_confidence,
        case
            when high_confidence_query_count > 0 then 'database.schema.object'
            when medium_confidence_query_count > 0 then 'schema.object'
            else 'object_name_only'
        end as attribution_method
    from query_stats
),

chain_context as (
    select
        ss.*,
        ch.downstream_table_count,
        ch.downstream_table_fqns,
        ch.min_hops_to_table,
        ch.model_fqn is not null as is_in_view_chain
    from scored_stats as ss
    left join {{ ref('int_snowflake__view_chains') }} as ch
        on ch.model_fqn = ss.table_fqn
),

downstream_build_stats as (
    select
        vc.model_fqn,
        avg(qh.execution_time_ms) / 1000.0 as downstream_build_time_s
    from (
        select distinct model_fqn, downstream_table_fqns
        from {{ ref('int_snowflake__view_chains') }}
        where array_size(downstream_table_fqns) > 0
    ) as vc,
    lateral flatten(input => vc.downstream_table_fqns) as fqn_flat
    join {{ ref('int_snowflake__query_history') }} as qh
        on qh.query_text ilike '%' || split_part(fqn_flat.value::string, '.', 3) || '%'
       and qh.query_type in ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE')
       and qh.query_start_time >= dateadd(month, -1, current_timestamp())
    group by vc.model_fqn
),

composite_scored as (
    select
        cc.*,
        coalesce(dbs.downstream_build_time_s, 0) as downstream_build_time_s,
        (
            greatest(coalesce(cc.select_count, 0), 1)
                * coalesce(cc.avg_gb_scanned_per_query, 0)
                * coalesce(cc.relative_duration_ratio, 1.0)
            + coalesce(dbs.downstream_build_time_s, 0)
        )
        * coalesce(cc.min_hops_to_table, 1)
        * greatest(coalesce(cc.downstream_table_count, 1), 1) as composite_chain_score
    from chain_context as cc
    left join downstream_build_stats as dbs
        on dbs.model_fqn = cc.table_fqn
),

final as (
    select
        current_date()                                               as snapshot_date,
        current_timestamp()                                          as analyzed_at,
        cs.table_fqn,
        cs.database_name,
        cs.schema_name,
        cs.table_name,
        cs.dbt_model,
        cs.model_name,
        cs.package_name,
        cs.materialized,
        coalesce(cs.select_count, 0)                                 as select_count,
        round(coalesce(cs.avg_query_duration_s, 0), 2)               as avg_query_duration_s,
        round(coalesce(cs.relative_duration_ratio, 0), 4)            as relative_duration_ratio,
        round(coalesce(cs.total_gb_scanned, 0), 4)                   as total_gb_scanned,
        round(coalesce(cs.avg_gb_scanned_per_query, 0), 6)           as avg_gb_scanned_per_query,
        round(coalesce(cs.materialization_score, 0), 2)              as materialization_score,
        cs.is_in_view_chain,
        cs.min_hops_to_table,
        cs.downstream_table_count,
        round(cs.composite_chain_score, 4)                           as composite_chain_score,
        round(coalesce(cs.downstream_build_time_s, 0), 2)            as downstream_build_time_s,
        cs.attribution_method,
        cs.attribution_confidence,
        coalesce(cs.high_confidence_query_count, 0)                  as high_confidence_query_count,
        coalesce(cs.medium_confidence_query_count, 0)                as medium_confidence_query_count,
        coalesce(cs.low_confidence_query_count, 0)                   as low_confidence_query_count,
        (
            iff(coalesce(cs.total_gb_scanned, 0) > 10, 1, 0)
            + iff(coalesce(cs.avg_query_duration_s, 0) > 10, 1, 0)
            + iff(coalesce(cs.select_count, 0) > greatest({{ min_query_count }}, 50), 1, 0)
            + iff(coalesce(cs.is_in_view_chain, false) and coalesce(cs.composite_chain_score, 0) > 0, 1, 0)
        )                                                            as strong_signal_count,
        case
            when cs.is_in_view_chain and coalesce(cs.composite_chain_score, 0) > 0
                then 'Materialize as TABLE'
            when not coalesce(cs.is_in_view_chain, false)
                and coalesce(cs.materialization_score, 0) > 500
                and coalesce(cs.total_gb_scanned, 0) > 10
                then 'Materialize as TABLE'
            when not coalesce(cs.is_in_view_chain, false)
                and coalesce(cs.avg_query_duration_s, 0) > 10
                and coalesce(cs.select_count, 0) > 50
                then 'Materialize as TABLE'
            else 'Monitor'
        end                                                          as recommendation,
        case
            when cs.is_in_view_chain and coalesce(cs.composite_chain_score, 0) > 0
                then cs.min_hops_to_table
                    || ' hop(s) from nearest downstream table with '
                    || cs.downstream_table_count
                    || ' downstream table(s) — materializing eliminates cascading recomputation'
            when not coalesce(cs.is_in_view_chain, false)
                and coalesce(cs.materialization_score, 0) > 500
                and coalesce(cs.total_gb_scanned, 0) > 10
                then 'High query volume with large data scan ('
                    || round(coalesce(cs.total_gb_scanned, 0), 2)
                    || ' GB) — repeated view computation is expensive; materializing eliminates redundant scans'
            when not coalesce(cs.is_in_view_chain, false)
                and coalesce(cs.avg_query_duration_s, 0) > 10
                and coalesce(cs.select_count, 0) > 50
                then 'Slow average query time ('
                    || round(coalesce(cs.avg_query_duration_s, 0), 1)
                    || 's) on a frequently queried view ('
                    || coalesce(cs.select_count, 0)
                    || ' queries) — materializing eliminates repeated computation'
            else 'Query volume or execution time below recommendation thresholds — continue monitoring'
        end                                                          as recommendation_reason
    from composite_scored as cs
    where coalesce(cs.select_count, 0) >= {{ min_query_count }}
       or coalesce(cs.is_in_view_chain, false)
)

select
    *,
    case
        when recommendation = 'Materialize as TABLE'
             and attribution_confidence = 'high'
             and strong_signal_count >= 2
            then 'high'
        when recommendation = 'Materialize as TABLE'
             and (attribution_confidence in ('high', 'medium') or strong_signal_count >= 2)
            then 'medium'
        else 'low'
    end as recommendation_confidence
from final
order by
    case when recommendation = 'Materialize as TABLE' then 0 else 1 end,
    case recommendation_confidence when 'high' then 0 when 'medium' then 1 else 2 end,
    coalesce(composite_chain_score, materialization_score, 0) desc
