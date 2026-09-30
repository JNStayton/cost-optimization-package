{{
  config(
    materialized='view',
  )
}}

{#--
  User-level cost attribution across three categories:
    a) Build users — ran dbt model builds (INSERT/MERGE/CTAS in dbt sessions)
    b) Consumption users — ran SELECT queries against project models
    c) AI users — used Cortex AI services

  Audience: Engineering managers, finance, platform teams.
  Scope: Scoped to monitored projects by default via dbt comment parsing (builds)
  and model FQN matching (consumption). AI users are account-wide.
--#}

{% set credit_rate_usd = var('credit_rate_usd', 2) %}
{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}
{% set monitor_all = (monitored_projects | length == 1 and monitored_projects[0] == '*') %}

with warehouse_list_rates as (
    -- Snowflake list rate per warehouse size, for fallback when QAH has no row.
    select
        warehouse_name,
        {{ warehouse_credits_per_hour('current_size') }} as credits_per_hour
    from {{ ref('int_snowflake__warehouse_config') }}
),

build_queries as (
    select distinct
        qh.user_name,
        qh.role_name,
        qh.query_id,
        qh.warehouse_name,
        -- Primary: exact attribution from QAH. Fallback: elapsed × list rate.
        coalesce(
            qah.credits_attributed_compute,
            qh.total_elapsed_time_ms / 1000.0
                * coalesce(wlr.credits_per_hour, 1) / 3600.0
        ) as query_credits,
        qah.credits_attributed_compute is not null as credits_from_attribution
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_snowflake__dbt_sessions') }} as s
        on qh.session_id = s.session_id
    left join {{ source('snowflake_usage', 'query_attribution_history') }} as qah
        on qh.query_id = qah.query_id
    left join warehouse_list_rates as wlr
        on wlr.warehouse_name = qh.warehouse_name
    where qh.query_start_time >= dateadd(day, -30, current_timestamp())
      and qh.query_type in ('INSERT', 'MERGE', 'CREATE_TABLE_AS_SELECT')
    {% if not monitor_all %}
      and qh.dbt_node_id is not null
      and split_part(qh.dbt_node_id, '.', 2) in (
          {% for proj in monitored_projects %}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
          {% endfor %}
      )
    {% endif %}
),

build_users as (
    select
        user_name,
        max(role_name) as role_name,
        round(sum(query_credits), 4) as build_credits_30d,
        count(distinct query_id) as build_query_count,
        max(warehouse_name) as primary_warehouse,
        boolor_agg(credits_from_attribution) as build_credits_from_attribution
    from build_queries
    group by user_name
),

consumption_queries as (
    select distinct
        qh.user_name,
        qh.role_name,
        qh.query_id,
        coalesce(
            qah.credits_attributed_compute,
            qh.total_elapsed_time_ms / 1000.0
                * coalesce(wlr.credits_per_hour, 1) / 3600.0
        ) as query_credits,
        qah.credits_attributed_compute is not null as credits_from_attribution
    from {{ ref('int_snowflake__query_history') }} as qh
    inner join {{ ref('int_dbt__relations') }} as dr
        on qh.query_text ilike '%' || dr.database_name || '.' || dr.schema_name || '.' || dr.table_name || '%'
    left join {{ source('snowflake_usage', 'query_attribution_history') }} as qah
        on qh.query_id = qah.query_id
    left join warehouse_list_rates as wlr
        on wlr.warehouse_name = qh.warehouse_name
    where qh.query_start_time >= dateadd(day, -30, current_timestamp())
      and qh.query_type = 'SELECT'
    {% if not monitor_all %}
      and dr.package_name in (
          {% for proj in monitored_projects %}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
          {% endfor %}
      )
    {% endif %}
),

consumption_users as (
    select
        user_name,
        max(role_name) as role_name,
        round(sum(query_credits), 4) as consumption_credits_30d,
        count(distinct query_id) as consumption_query_count,
        boolor_agg(credits_from_attribution) as consumption_credits_from_attribution
    from consumption_queries
    group by user_name
),

ai_users as (
    select
        user_name,
        default_role as role_name,
        total_credits_30d as ai_credits_30d,
        total_queries_30d as ai_query_count
    from {{ ref('fct_snowflake__ai_user_spend_recommendations') }}
    where user_name is not null
)

select
    coalesce(bu.user_name, cu.user_name, au.user_name) as user_name,
    coalesce(bu.role_name, cu.role_name, au.role_name) as role_name,
    coalesce(bu.build_credits_30d, 0) as build_credits_30d,
    coalesce(cu.consumption_credits_30d, 0) as consumption_credits_30d,
    coalesce(au.ai_credits_30d, 0) as ai_credits_30d,
    coalesce(bu.build_credits_30d, 0)
        + coalesce(cu.consumption_credits_30d, 0)
        + coalesce(au.ai_credits_30d, 0) as combined_credits_30d,
    round(
        (coalesce(bu.build_credits_30d, 0)
         + coalesce(cu.consumption_credits_30d, 0)
         + coalesce(au.ai_credits_30d, 0))
        * 12 * {{ credit_rate_usd }}
    , 2) as estimated_annual_cost_usd,
    bu.primary_warehouse,
    coalesce(bu.build_query_count, 0) as build_query_count,
    coalesce(cu.consumption_query_count, 0) as consumption_query_count,
    coalesce(au.ai_query_count, 0) as ai_query_count,
    case
        when bu.user_name is not null and cu.user_name is not null then 'mixed'
        when bu.user_name is not null then 'builder'
        when cu.user_name is not null then 'consumer'
        when au.user_name is not null then 'ai_user'
        else 'unknown'
    end as user_category,
    case
        when coalesce(bu.build_credits_30d, 0)
             + coalesce(cu.consumption_credits_30d, 0)
             + coalesce(au.ai_credits_30d, 0) > 10
            then 'High cost user — review workload patterns'
        when coalesce(bu.build_credits_30d, 0)
             + coalesce(cu.consumption_credits_30d, 0)
             + coalesce(au.ai_credits_30d, 0) > 2
            then 'Moderate cost user — monitor trends'
        else 'Low cost user'
    end as recommendation,
    coalesce(bu.build_credits_from_attribution, false)
        or coalesce(cu.consumption_credits_from_attribution, false) as credits_from_attribution
from build_users as bu
full outer join consumption_users as cu
    on cu.user_name = bu.user_name
full outer join ai_users as au
    on au.user_name = coalesce(bu.user_name, cu.user_name)
order by combined_credits_30d desc
