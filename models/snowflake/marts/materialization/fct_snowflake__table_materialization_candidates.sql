{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

{#--
  dbt VIEW and EPHEMERAL models that are candidates for conversion to TABLE
  materialization, based on query volume, data scan cost, execution time relative
  to project average, and view/ephemeral chain analysis.

  For models in a view/ephemeral chain, any model with composite_chain_score > 0
  receives the recommendation. composite_chain_score is:
    (greatest(select_count, 1) * avg_gb_scanned_per_query * relative_duration_ratio
      + downstream_build_time_s) * min_hops_to_table * greatest(downstream_table_count, 1)

  For isolated views not in any chain, existing materialization_score thresholds apply.

  ACCESS_HISTORY resolves views to their base tables, so query attribution here
  uses query_text ILIKE matching against the view name regardless of edition.
  Known limitation: view names that are substrings of other identifiers may
  produce false-positive query matches.

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

query_stats as (
    select
        vc.table_fqn,
        vc.database_name,
        vc.schema_name,
        vc.table_name,
        vc.dbt_model,
        vc.model_name,
        vc.package_name,
        vc.materialized,
        count(distinct qh.query_id)                                        as select_count,
        avg(qh.execution_time_ms) / 1000.0                                 as avg_query_duration_s,
        sum(coalesce(qh.bytes_scanned, 0)) / power(1024, 3)               as total_gb_scanned,
        count(distinct qh.query_id) * avg(qh.execution_time_ms) / 1000.0  as materialization_score,
        coalesce(
            sum(coalesce(qh.bytes_scanned, 0)) / power(1024, 3)
                / nullif(count(distinct qh.query_id), 0),
            0
        )                                                                   as avg_gb_scanned_per_query
    from view_candidates as vc
    left join {{ ref('int_snowflake__query_history') }} as qh
        on qh.query_text ilike '%' || vc.table_name || '%'
       and qh.query_type = 'SELECT'
       and qh.query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
    group by
        vc.table_fqn,
        vc.database_name,
        vc.schema_name,
        vc.table_name,
        vc.dbt_model,
        vc.model_name,
        vc.package_name,
        vc.materialized
),

scored_stats as (
    -- adds relative_duration_ratio: how this view's duration compares to the
    -- project-wide average, normalizing out warehouse size differences
    select
        *,
        avg_query_duration_s
            / nullif(avg(avg_query_duration_s) over (), 0) as relative_duration_ratio
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
    -- avg build time of downstream tables per view, using the pre-aggregated
    -- fqn array from view_chains to avoid fan-out
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
)

select
    current_date()                                              as snapshot_date,
    current_timestamp()                                        as analyzed_at,
    cs.table_fqn,
    cs.database_name,
    cs.schema_name,
    cs.table_name,
    cs.dbt_model,
    cs.model_name,
    cs.package_name,
    cs.materialized,
    coalesce(cs.select_count, 0)                              as select_count,
    round(coalesce(cs.avg_query_duration_s, 0), 2)            as avg_query_duration_s,
    round(coalesce(cs.relative_duration_ratio, 0), 4)         as relative_duration_ratio,
    round(coalesce(cs.total_gb_scanned, 0), 4)                as total_gb_scanned,
    round(coalesce(cs.avg_gb_scanned_per_query, 0), 6)        as avg_gb_scanned_per_query,
    round(coalesce(cs.materialization_score, 0), 2)           as materialization_score,
    cs.is_in_view_chain,
    cs.min_hops_to_table,
    cs.downstream_table_count,
    round(cs.composite_chain_score, 4)                        as composite_chain_score,
    round(coalesce(cs.downstream_build_time_s, 0), 2)         as downstream_build_time_s,
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
    end                                                        as recommendation,
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
    end                                                        as recommendation_reason
from composite_scored as cs
where coalesce(cs.select_count, 0) >= {{ min_query_count }}
   or coalesce(cs.is_in_view_chain, false)
order by
    case when recommendation = 'Materialize as TABLE' then 0 else 1 end,
    coalesce(cs.composite_chain_score, cs.materialization_score, 0) desc
