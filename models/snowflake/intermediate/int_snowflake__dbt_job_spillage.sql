{{
  config(
    materialized='table',
  )
}}

{#--
  Job-level spillage: one row per dbt job, over the spillage_lookback_days window. When a
  few models dominate a job's build time by spilling, route those models to a larger
  warehouse; when spill is widespread, size up the job's warehouse instead. The two are
  alternatives, so each job gets at most one signal.

  Job and run come from dbt's query comment: dbt_cloud_job_id and dbt_cloud_run_id (dbt
  platform jobs). Runs outside the dbt platform carry no job id; when the query comment
  includes invocation_id (add it to dbt's query-comment), each invocation is treated as
  a one-run job. Builds are queries tagged with a model's node_id.

    spilling_model_share_pct  models that spilled / models built (distinct, over the window)
    spilling_time_share_pct   build time of the models that spilled / all build time

  Rule (vars):
    time share < spillage_job_min_time_share_pct (25)              → no signal
    model share <= spillage_job_routing_max_model_share_pct (25)  → spillage_route_models
    model share >  spillage_job_routing_max_model_share_pct        → spillage_job_scale_up
    time share >= spillage_job_actionable_time_share_pct (75)       → actionable, else monitor
--#}

{% set lookback_days = var('spillage_lookback_days', 30) %}
{% set min_time_share = var('spillage_job_min_time_share_pct', 25) %}
{% set actionable_time_share = var('spillage_job_actionable_time_share_pct', 75) %}
{% set routing_max_model_share = var('spillage_job_routing_max_model_share_pct', 25) %}

with build_queries as (
    select
        coalesce(
            qh.dbt_cloud_job_id,
            'invocation:' || try_parse_json(regexp_substr(qh.query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):invocation_id::string
        )                                                           as job_key,
        qh.dbt_cloud_job_id,
        coalesce(
            qh.dbt_cloud_run_id,
            try_parse_json(regexp_substr(qh.query_text, '/\\*\\s*(\\{.+\\})\\s*\\*/', 1, 1, 'e')):invocation_id::string
        )                                                           as run_key,
        qh.dbt_node_id                                              as node_id,
        qh.warehouse_name,
        coalesce(qh.execution_time_ms, 0) / 1000.0                  as execution_s,
        coalesce(qh.bytes_spilled_local, 0) + coalesce(qh.bytes_spilled_remote, 0) as bytes_spilled
    from {{ ref('int_snowflake__query_history') }} as qh
    where qh.dbt_node_id like 'model.%'
      and cast(qh.query_start_time as date) >= dateadd(day, -{{ lookback_days }}, current_date())
),

job_builds as (
    select * from build_queries where job_key is not null and run_key is not null
),

job_models as (
    select
        job_key,
        node_id,
        sum(execution_s)        as build_s,
        sum(bytes_spilled)      as bytes_spilled,
        sum(bytes_spilled) > 0  as spilled
    from job_builds
    group by job_key, node_id
),

job_warehouse as (
    select job_key, warehouse_name
    from job_builds
    where warehouse_name is not null
    group by job_key, warehouse_name
    qualify row_number() over (partition by job_key order by sum(execution_s) desc, warehouse_name) = 1
),

jobs as (
    select
        jm.job_key,
        max(jr.dbt_cloud_job_id)                                    as dbt_cloud_job_id,
        max(jr.run_count)                                           as run_count,
        count(distinct jm.node_id)                                  as models_built,
        count(distinct iff(jm.spilled, jm.node_id, null))           as models_spilling,
        sum(jm.build_s)                                             as build_s,
        sum(iff(jm.spilled, jm.build_s, 0))                         as spilling_build_s,
        round(sum(jm.bytes_spilled) / power(1024, 3), 2)            as total_gb_spilled,
        array_agg(iff(jm.spilled, object_construct(
            'node_id', jm.node_id, 'build_s', jm.build_s,
            'gb_spilled', round(jm.bytes_spilled / power(1024, 3), 2)), null))
            within group (order by jm.build_s desc)                 as spilling_models
    from job_models as jm
    inner join (
        select job_key, max(dbt_cloud_job_id) as dbt_cloud_job_id, count(distinct run_key) as run_count
        from job_builds
        group by job_key
    ) as jr
        on jr.job_key = jm.job_key
    group by jm.job_key
),

shares as (
    select
        j.*,
        round(100 * j.models_spilling / nullif(j.models_built, 0), 1)   as spilling_model_share_pct,
        round(100 * j.spilling_build_s / nullif(j.build_s, 0), 1)       as spilling_time_share_pct,
        jw.warehouse_name,
        wc.current_size                                                 as warehouse_current_size,
        -- Other jobs on the same warehouse: resizing it would change their cost too
        (select count(distinct o.job_key) from job_builds as o
         where o.warehouse_name = jw.warehouse_name and o.job_key != j.job_key) > 0 as warehouse_shared
    from jobs as j
    left join job_warehouse as jw on jw.job_key = j.job_key
    left join {{ ref('int_snowflake__warehouse_config') }} as wc on wc.warehouse_name = jw.warehouse_name
),

-- Warehouses one size up from each job's warehouse (twice the credit rate), for routing:
-- ones already running dbt builds first, at most five.
candidates as (
    select
        job_key,
        listagg(warehouse_name, ', ') within group (order by candidate_rank) as candidate_warehouses
    from (
        select
            s.job_key,
            c.warehouse_name,
            row_number() over (partition by s.job_key order by c.dbt_build_count desc, c.warehouse_name)
                as candidate_rank
        from shares as s
        inner join (
            select
                wc.warehouse_name,
                {{ warehouse_credits_per_hour('wc.current_size') }} as credits_per_hour,
                coalesce(b.dbt_build_count, 0) as dbt_build_count
            from {{ ref('int_snowflake__warehouse_config') }} as wc
            left join (
                select warehouse_name, count(*) as dbt_build_count
                from build_queries
                group by warehouse_name
            ) as b on b.warehouse_name = wc.warehouse_name
        ) as c
            on c.credits_per_hour = 2 * {{ warehouse_credits_per_hour('s.warehouse_current_size') }}
    )
    where candidate_rank <= 5
    group by job_key
)

select
    s.job_key,
    s.dbt_cloud_job_id,
    s.run_count,
    s.warehouse_name,
    s.warehouse_current_size,
    s.warehouse_shared,
    s.models_built,
    s.models_spilling,
    s.spilling_model_share_pct,
    s.build_s,
    s.spilling_build_s,
    s.spilling_time_share_pct,
    s.total_gb_spilled,
    s.spilling_models,
    ct.candidate_warehouses,
    {{ next_warehouse_size('s.warehouse_current_size', 'up') }}     as next_warehouse_size,
    {{ warehouse_scale_up_efficiency('s.warehouse_current_size') }} as scale_up_efficiency,
    case
        when s.spilling_time_share_pct < {{ min_time_share }} or s.models_spilling = 0 then null
        when s.spilling_model_share_pct <= {{ routing_max_model_share }} then 'spillage_route_models'
        else 'spillage_job_scale_up'
    end                                                             as signal_id,
    case
        when s.spilling_time_share_pct < {{ min_time_share }} or s.models_spilling = 0 then null
        when s.spilling_time_share_pct >= {{ actionable_time_share }} then 'actionable'
        else 'monitor'
    end                                                             as backlog_status,
    s.models_spilling || ' of ' || s.models_built || ' models ('
        || to_varchar(round(s.spilling_model_share_pct)) || '%) took '
        || to_varchar(round(s.spilling_time_share_pct)) || '% of this job''s build time'
        || ' (' || s.run_count || ' run(s) in ' || {{ lookback_days }} || ' days).' as evidence,
    current_date()                                                  as snapshot_date
from shares as s
left join candidates as ct on ct.job_key = s.job_key
