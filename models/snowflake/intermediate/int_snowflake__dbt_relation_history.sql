{{
  config(
    materialized='table',
  )
}}

{#--
  Cross-environment dbt relation history.

  Discovers all physical materializations of dbt models across environments
  (dev, staging, prod) by parsing DDL statements from the staged query history.

  Maps each logical dbt model (node_id) to every physical FQN where it has been
  materialized. Enables cross-environment recommendation deduplication and
  identification of models that exist in non-prod but haven't reached prod yet.

  Grain: one row per physical relation (table_fqn). A table built under several target
  names (e.g. a dbt platform Studio session that appears as both 'default' and 'dev')
  is one deployment, not several: target_name is the latest build's target, and
  target_names / dbt_cloud_environment_ids list every one seen. Keeping one row per
  table keeps every join on table_fqn one-to-one, so recommendations aren't multiplied.

  Rebuilt as a table on every run from stg_snowflake__query_history (which keeps the
  history), over dbt_relation_history_lookback_days. An incremental merge would
  overwrite first_built_at, build_count and the lists with only the re-scanned window.

  is_excluded: the dbt_excluded_schemas / dbt_excluded_targets vars (see
  relation_is_excluded). Excluded deployments are left out of recommendations and
  environment counts.
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
        dbt_node_id as node_id,
        dbt_target_name as target_name,
        dbt_cloud_environment_id,
        -- Extract the materialized FQN from the DDL statement
        -- Handles: CREATE [OR REPLACE] [TRANSIENT] TABLE|VIEW db.schema.name
        upper(regexp_substr(query_text, '(view|table)\\s+([a-z0-9_]+\\.[a-z0-9_]+\\.[a-z0-9_]+)', 1, 1, 'ie', 2)) as ddl_fqn
    from {{ ref('stg_snowflake__query_history') }}
    where dbt_node_id is not null
      and query_type in ('CREATE_TABLE_AS_SELECT', 'CREATE_VIEW', 'INSERT', 'MERGE')
      and start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
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
    where node_id like 'model.%'
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
        table_fqn,
        max_by(node_id, start_time) as node_id,
        max_by(project_name, start_time) as project_name,
        max_by(model_name, start_time) as model_name,
        max_by(target_name, start_time) as target_name,
        array_agg(distinct target_name) within group (order by target_name) as target_names,
        split_part(table_fqn, '.', 1) as database_name,
        split_part(table_fqn, '.', 2) as schema_name,
        split_part(table_fqn, '.', 3) as table_name,
        max(dbt_cloud_environment_id) as dbt_cloud_environment_id,
        array_agg(distinct dbt_cloud_environment_id) within group (order by dbt_cloud_environment_id)
            as dbt_cloud_environment_ids,
        min(start_time) as first_built_at,
        max(start_time) as last_built_at,
        count(*) as build_count
    from parsed
    group by table_fqn
)

select
    a.node_id,
    a.table_fqn,
    a.project_name,
    a.model_name,
    a.target_name,
    a.target_names,
    a.database_name,
    a.schema_name,
    a.table_name,
    a.dbt_cloud_environment_id,
    a.dbt_cloud_environment_ids,
    a.first_built_at,
    a.last_built_at,
    a.build_count,
    -- Flag whether this FQN matches the current compilation target
    case
        when dr.table_fqn is not null then true
        else false
    end as is_current_target,
    {{ relation_is_excluded('a.schema_name', 'a.target_names') }} as is_excluded
from aggregated as a
left join {{ ref('int_dbt__relations') }} as dr
    on a.table_fqn = dr.table_fqn
