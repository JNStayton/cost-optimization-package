{{
  config(
    materialized='view',
  )
}}

{#--
  Top queried models by downstream SELECT activity (consumption).
  Shows which models are most heavily used by dashboards, BI tools, and ad-hoc queries.
  Useful for identifying high-impact models where optimizations have outsized ROI.

  Audience: dbt developers, platform engineers.
  Grain: one row per model (table_fqn).
  Ordered by total SELECT count descending.
--#}

{% set lookback_days = var('top_models_lookback_days', 30) %}

with query_stats as (
    select
        table_database || '.' || table_schema || '.' || table_name as table_fqn,
        sum(select_count) as total_selects,
        round(sum(select_count) * 1.0 / {{ lookback_days }}, 1) as avg_daily_selects,
        round(sum(select_execution_time_ms_sum) / 1000.0, 1) as total_select_time_sec,
        round(sum(select_execution_time_ms_sum) / nullif(sum(select_count), 0) / 1000.0, 2) as avg_query_duration_sec,
        round(sum(bytes_scanned_sum) / power(1024, 3), 2) as total_gb_scanned,
        count(distinct stats_date) as active_days
    from {{ ref('int_snowflake__table_query_stats_daily') }}
    where stats_date >= dateadd(day, -{{ lookback_days }}, current_date())
      and select_count > 0
    group by 1
),

enriched as (
    select
        qs.*,
        dr.dbt_model as node_id,
        dr.model_name,
        dr.package_name as project_name
    from query_stats as qs
    inner join {{ ref('int_dbt__relations') }} as dr
        on dr.table_fqn = qs.table_fqn
    where dr.package_name in (
        {%- set monitored_projects = var('dbt_monitored_projects', []) -%}
        {%- if monitored_projects | length == 0 -%}
            '{{ project_name }}'
        {%- else -%}
            {%- for proj in monitored_projects -%}
                '{{ proj }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        {%- endif -%}
    )
)

select
    node_id,
    model_name,
    project_name,
    table_fqn,
    total_selects as total_selects_30d,
    avg_daily_selects,
    total_select_time_sec,
    avg_query_duration_sec,
    total_gb_scanned,
    active_days,
    current_date() as snapshot_date
from enriched
where total_selects > 0
order by total_selects desc
limit 25
