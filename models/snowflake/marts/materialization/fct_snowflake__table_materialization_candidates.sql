{{
  config(
    materialized='table',
  )
}}

{#--
  V2 of table materialization candidates.

  Keeps the existing recommendation logic intact, but makes query-text attribution
  more transparent by classifying each matched query into a single attribution tier:
    - high:   fully-qualified DATABASE.SCHEMA.OBJECT match (or ACCESS_HISTORY for Enterprise+)
    - medium: schema-qualified SCHEMA.OBJECT match in query_text
    - low:    bare OBJECT name match only

  Enterprise+ customers use ACCESS_HISTORY (direct_objects_accessed) for exact FQN
  attribution — all matches are automatically "high" confidence.
  Standard edition customers fall back to ILIKE text-matching against query_text.

  Also adds recommendation_confidence based on the quantity of corroborating
  signals rather than any single signal in isolation.

  Controlled by the following dbt variables:
    - table_materialization_lookback_days   (default 14)
    - table_materialization_min_query_count (default 10)
    - snowflake_enterprise_edition          (default true) — controls attribution path
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
    -- Views only: an ephemeral has no relation to read, build, or materialize on its own.
    where lower(materialized) = 'view'
),

{% if var('snowflake_enterprise_edition', true) %}
-- Enterprise+ path: use ACCESS_HISTORY for exact FQN attribution
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
        reads.query_id,
        reads.execution_time_ms,
        coalesce(reads.bytes_scanned, 0) as bytes_scanned,
        iff(reads.query_id is not null, 'high', null) as attribution_confidence
    from view_candidates as vc
    -- Left join: a view in a chain is a candidate even when nothing reads it directly,
    -- since every downstream table build recomputes it (as on Standard edition).
    left join (
        select doa.table_fqn, doa.query_id, qh.execution_time_ms, qh.bytes_scanned
        from {{ ref('int_snowflake__direct_object_access') }} as doa
        inner join {{ ref('int_snowflake__query_history') }} as qh
            on qh.query_id = doa.query_id
            and qh.query_type = 'SELECT'
        where doa.query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
    ) as reads
        on reads.table_fqn = vc.table_fqn
),
{% else %}
-- Standard edition fallback: ILIKE text-matching against query_text
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
{% endif %}

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

downstream_builds as (
    -- Builds, in the lookback window, of every table downstream of this view through
    -- views and ephemerals. Each one recomputes the view's query inline. Counted by the
    -- downstream model's node_id in dbt's query comment: CTAS and MERGE statements, the
    -- ones that evaluate the model's SQL (dbt's INSERT normally reads from a __dbt_tmp
    -- relation). Every view in a chain is credited with the same builds; the views are
    -- alternatives to each other, and chain_selection below recommends one of them.
    select
        p.upstream_fqn              as model_fqn,
        count(distinct qh.query_id) as downstream_build_count
    from {{ ref('int_snowflake__view_chain_pairs') }} as p
    inner join {{ ref('int_snowflake__query_history') }} as qh
        on qh.dbt_node_id = p.table_dbt_model
       and qh.query_type in ('CREATE_TABLE_AS_SELECT', 'MERGE')
       and qh.query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
    group by p.upstream_fqn
),

view_builds as (
    -- How often dbt (re)created each view in the lookback window. After materializing,
    -- each of these runs becomes a table build.
    select
        dbt_node_id,
        count(distinct query_id) as view_build_runs
    from {{ ref('int_snowflake__query_history') }}
    where query_type = 'CREATE_VIEW'
      and dbt_node_id is not null
      and query_start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
    group by dbt_node_id
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
        coalesce(db.downstream_build_count, 0)                       as downstream_build_count,
        greatest(coalesce(vb.view_build_runs, 0), 1)                 as view_build_runs,
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
    left join downstream_builds as db
        on db.model_fqn = cs.table_fqn
    left join view_builds as vb
        on vb.dbt_node_id = cs.dbt_model
    where coalesce(cs.select_count, 0) >= {{ min_query_count }}
       or coalesce(cs.is_in_view_chain, false)
),

costed as (
    -- Recompute cost: the view probe's measured execution time when there is one
    -- (int_snowflake__view_probe), else the average read duration.
    select
        f.*,
        case
            when pr.view_fqn is not null then round(pr.execution_time_ms / 1000.0, 3)
            when f.select_count > 0 then f.avg_query_duration_s
        end as recompute_cost_s,
        case
            when pr.view_fqn is not null then 'probe'
            when f.select_count > 0 then 'reads'
        end as recompute_cost_source
    from final as f
    left join {{ ref('int_snowflake__view_probe') }} as pr
        on pr.view_fqn = f.table_fqn
       and pr.probe_status = 'ok'
),

net_scored as (
    -- Seconds of view recompute removed per lookback window by materializing: every read
    -- and downstream build stops recomputing the view, and each dbt run builds it once.
    select
        c.*,
        greatest(c.select_count + c.downstream_build_count - c.view_build_runs, 0)
            * coalesce(c.recompute_cost_s, 0) as net_recompute_s_saved
    from costed as c
),

chain_ranks as (
    -- For each table at the end of a view chain, rank the candidate views feeding it:
    -- highest net savings first, ties to the view nearest the table (it covers the most
    -- upstream work).
    select
        p.table_fqn          as chain_table_fqn,
        p.table_model_name   as chain_table_model_name,
        p.upstream_fqn,
        p.upstream_model_name,
        p.path_length,
        row_number() over (
            partition by p.table_fqn
            order by ns.net_recompute_s_saved desc, p.path_length, p.upstream_fqn
        ) as chain_rank
    from {{ ref('int_snowflake__view_chain_pairs') }} as p
    inner join net_scored as ns
        on ns.table_fqn = p.upstream_fqn
       and ns.recommendation = 'Materialize as TABLE'
),

chain_alternatives as (
    -- A view that wins for no table is an alternative to the winner of its nearest table.
    select
        cr.upstream_fqn,
        w.upstream_fqn          as chosen_view_fqn,
        w.upstream_model_name   as chosen_model_name,
        cr.chain_table_model_name,
        w.path_length < cr.path_length as chosen_is_nearer_table
    from chain_ranks as cr
    inner join chain_ranks as w
        on w.chain_table_fqn = cr.chain_table_fqn
       and w.chain_rank = 1
    where cr.upstream_fqn not in (select upstream_fqn from chain_ranks where chain_rank = 1)
    qualify row_number() over (partition by cr.upstream_fqn order by cr.path_length, cr.chain_table_fqn) = 1
),

chain_selection as (
    select
        ns.*,
        case
            when ns.table_fqn in (select upstream_fqn from chain_ranks where chain_rank = 1) then 'recommended'
            when ca.upstream_fqn is not null then 'alternative'
            else 'standalone'
        end as chain_role,
        case
            when ns.table_fqn in (select upstream_fqn from chain_ranks where chain_rank = 1) then ns.table_fqn
            else ca.chosen_view_fqn
        end as chosen_view_for_chain,
        ca.chosen_model_name,
        ca.chain_table_model_name,
        ca.chosen_is_nearer_table,
        (select count(distinct alt.upstream_fqn) from chain_alternatives as alt
         where alt.chosen_view_fqn = ns.table_fqn) as alternative_view_count
    from net_scored as ns
    left join chain_alternatives as ca
        on ca.upstream_fqn = ns.table_fqn
),

chain_reasoned as (
    select
        cs.* exclude (recommendation_reason, chosen_model_name, chain_table_model_name,
                      chosen_is_nearer_table, alternative_view_count),
        case
            when cs.chain_role = 'alternative' and cs.chosen_is_nearer_table
                then 'Alternative to materializing ' || cs.chosen_model_name
                    || ', which also removes this view''s recompute in '
                    || cs.chain_table_model_name || ' builds.'
            when cs.chain_role = 'alternative'
                then 'Alternative to materializing ' || cs.chosen_model_name
                    || ', which saves more on ' || cs.chain_table_model_name
                    || ' builds. Their savings overlap, so they aren''t added together.'
            when cs.chain_role = 'recommended' and cs.alternative_view_count > 0
                then cs.recommendation_reason || '. Chosen over ' || cs.alternative_view_count
                    || ' other view(s) in the same chain, which are listed as alternatives.'
            else cs.recommendation_reason
        end as recommendation_reason
    from chain_selection as cs
)

select
    f.*,
    case
        when f.recommendation = 'Materialize as TABLE'
             and f.attribution_confidence = 'high'
             and f.strong_signal_count >= 2
            then 'high'
        when f.recommendation = 'Materialize as TABLE'
             and (f.attribution_confidence in ('high', 'medium') or f.strong_signal_count >= 2)
            then 'medium'
        else 'low'
    end as recommendation_confidence,
    rh.node_id,
    rh.target_name,
    rh.project_name as node_project_name,
    -- Deployments of the model: distinct physical tables, excluding excluded ones
    (select count(distinct rh2.table_fqn) from {{ ref('int_snowflake__dbt_relation_history') }} rh2
     where rh2.node_id = rh.node_id and not coalesce(rh2.is_excluded, false)) as deployed_relation_count
from chain_reasoned as f
left join {{ ref('int_snowflake__dbt_relation_history') }} as rh
    on rh.table_fqn = f.table_fqn
order by
    case when f.recommendation = 'Materialize as TABLE' then 0 else 1 end,
    case
        when f.recommendation = 'Materialize as TABLE'
             and f.attribution_confidence = 'high'
             and f.strong_signal_count >= 2 then 0
        when f.recommendation = 'Materialize as TABLE'
             and (f.attribution_confidence in ('high', 'medium') or f.strong_signal_count >= 2) then 1
        else 2
    end,
    coalesce(f.composite_chain_score, f.materialization_score, 0) desc
