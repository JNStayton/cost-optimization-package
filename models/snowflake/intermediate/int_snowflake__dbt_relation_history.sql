{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['node_id', 'table_fqn'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Cross-environment dbt relation history.

  Parses dbt query comment JSON from QUERY_HISTORY to discover all physical
  materializations of dbt models across environments (dev, staging, prod).

  The dbt query comment format:
    /* {"app": "dbt", "node_id": "model.project.name", "target_name": "prod", ...} */

  This model maps each logical dbt model (node_id) to every physical FQN where
  it has been materialized. Enables cross-environment recommendation deduplication
  and identification of models that exist in non-prod but haven't reached prod yet.

  Grain: one row per (node_id, table_fqn)
--#}

{% set lookback_days = var('dbt_relation_history_lookback_days', 90) %}

{% set monitored_projects = var('dbt_monitored_projects', []) %}
{% if monitored_projects | length == 0 %}
  {% set monitored_projects = [project_name] %}
{% endif %}

with dbt_build_queries as (
    select
        query_id,
        start_time,
        query_text,
        query_type,
        -- Extract dbt comment JSON fields
        regexp_substr(query_text, '"node_id":\\s*"([^"]+)"', 1, 1, 'e') as node_id,
        regexp_substr(query_text, '"target_name":\\s*"([^"]+)"', 1, 1, 'e') as target_name,
        regexp_substr(query_text, '"dbt_cloud_environment_id":\\s*"([^"]+)"', 1, 1, 'e') as dbt_cloud_environment_id,
        -- Extract the materialized FQN from the DDL statement
        -- Handles: CREATE [OR REPLACE] [TRANSIENT] TABLE|VIEW db.schema.name
        upper(regexp_substr(query_text, '(view|table)\\s+([a-z0-9_]+\\.[a-z0-9_]+\\.[a-z0-9_]+)', 1, 1, 'ie', 2)) as ddl_fqn
    from {{ source('snowflake_usage', 'query_history') }}
    where query_text ilike '%"app": "dbt"%'
      and query_type in ('CREATE_TABLE_AS_SELECT', 'CREATE_VIEW', 'INSERT', 'MERGE')
      {% if is_incremental() %}
        and start_time >= (select dateadd(day, -1, max(last_built_at)) from {{ this }})
      {% else %}
        and start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
      {% endif %}
),

parsed as (
    select
        query_id,
        start_time,
        node_id,
        target_name,
        dbt_cloud_environment_id,
        -- Clean the FQN: remove __dbt_tmp suffix if present
        case
            when ddl_fqn like '%__DBT_TMP' then left(ddl_fqn, length(ddl_fqn) - 9)
            else ddl_fqn
        end as table_fqn,
        split_part(node_id, '.', 2) as project_name,
        split_part(node_id, '.', 3) as model_name
    from dbt_build_queries
    where node_id is not null
      and node_id like 'model.%'
      and ddl_fqn is not null
      and ddl_fqn not like '%__DBT_BACKUP'
      -- Filter to monitored projects
      and split_part(node_id, '.', 2) in (
          {% for proj in monitored_projects %}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
          {% endfor %}
      )
),

aggregated as (
    select
        node_id,
        table_fqn,
        project_name,
        model_name,
        target_name,
        split_part(table_fqn, '.', 1) as database_name,
        split_part(table_fqn, '.', 2) as schema_name,
        split_part(table_fqn, '.', 3) as table_name,
        max(dbt_cloud_environment_id) as dbt_cloud_environment_id,
        min(start_time) as first_built_at,
        max(start_time) as last_built_at,
        count(*) as build_count
    from parsed
    group by node_id, table_fqn, project_name, model_name, target_name,
             split_part(table_fqn, '.', 1),
             split_part(table_fqn, '.', 2),
             split_part(table_fqn, '.', 3)
)

select
    a.node_id,
    a.table_fqn,
    a.project_name,
    a.model_name,
    a.target_name,
    a.database_name,
    a.schema_name,
    a.table_name,
    a.dbt_cloud_environment_id,
    a.first_built_at,
    a.last_built_at,
    a.build_count,
    -- Flag whether this FQN matches the current compilation target
    case
        when dr.table_fqn is not null then true
        else false
    end as is_current_target
from aggregated as a
left join {{ ref('int_dbt__relations') }} as dr
    on a.table_fqn = dr.table_fqn
