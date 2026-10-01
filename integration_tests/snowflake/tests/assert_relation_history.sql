{#-
  int_snowflake__dbt_relation_history has one row per physical table, so joins on the
  table name can't multiply recommendations:
    - demo_orders is built under target names 'default' and 'dev' into the same schema:
      one row, both targets listed, 14 builds.
    - demo_orders also has a second deployment (<schema>_deploy, target 'prod',
      environment 12345): a separate row. The model's environment_count is 2.
    - No table appears twice, and neither does a materialization candidate.
  Exclusion (dbt_excluded_targets / dbt_excluded_schemas): demo_logs is built only under
  'dev'. With dbt_excluded_targets: ['dev'] it is excluded and has no recommendation;
  demo_orders (also built under 'default') is not. By default nothing is excluded.
  Returns one row per failed check.
-#}
{%- set orders_fqn = (target.database ~ '.' ~ target.schema ~ '.demo_orders') | upper %}
{%- set deploy_fqn = (target.database ~ '.' ~ target.schema ~ '_deploy.demo_orders') | upper %}
{%- set logs_fqn = (target.database ~ '.' ~ target.schema ~ '.demo_logs') | upper %}
{%- set dev_excluded = 'dev' in var('dbt_excluded_targets', []) %}

with rh as (
    select * from {{ ref('int_snowflake__dbt_relation_history') }}
),

checks as (
    select 'one row per table' as check_name,
           (select count(*) - count(distinct table_fqn) from rh)::varchar as produced, '0' as expected
    union all
    select 'demo_orders: targets and builds',
           (select array_to_string(target_names, ',') || ' / ' || build_count from rh where table_fqn = '{{ orders_fqn }}'),
           'default,dev / 14'
    union all
    select 'demo_orders deploy: target and environment',
           (select target_name || ' / ' || array_to_string(dbt_cloud_environment_ids, ',') from rh where table_fqn = '{{ deploy_fqn }}'),
           'prod / 12345'
    union all
    select 'demo_orders environment_count',
           (select max(environment_count) from {{ ref('vw_snowflake__dbt_model_optimizations') }}
            where model_name = 'demo_orders')::varchar,
           '2'
    union all
    select 'materialization candidates: one row per table',
           (select count(*) - count(distinct table_fqn)
            from {{ ref('fct_snowflake__table_materialization_candidates') }})::varchar,
           '0'
    union all
    select 'demo_logs is_excluded',
           (select is_excluded from rh where table_fqn = '{{ logs_fqn }}')::varchar,
           '{{ "true" if dev_excluded else "false" }}'
    union all
    select 'demo_logs recommendations',
           (select count(*) from {{ ref('int_snowflake__all_recommendations') }}
            where table_fqn = '{{ logs_fqn }}' and domain = 'materialization')::varchar,
           '{{ "0" if dev_excluded else "1" }}'
    union all
    select 'demo_orders still recommended',
           (select (count(*) > 0)::varchar from {{ ref('int_snowflake__all_recommendations') }}
            where table_fqn = '{{ orders_fqn }}' and domain = 'materialization'),
           'true'
)

select * from checks where produced is distinct from expected
