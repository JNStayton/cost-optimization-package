{#-
  refresh_warehouse_config (post-hook on int_snowflake__warehouse_config) merges live
  SHOW WAREHOUSES settings. The fixture warehouses don't exist live, so they must keep null
  settings (the recommendations then fall back to 300 s auto-suspend). The target's own
  warehouse does exist, so it must have its live auto-suspend, and the Standard edition
  scaling policy. Also checks the auto-suspend cycle count for FIXTURE_WH_IDLE.
  Returns one row per failed check.
-#}
-- depends_on: {{ ref('int_snowflake__warehouse_config') }}
with config as (
    select * from {{ ref('int_snowflake__warehouse_config') }}
),

checks as (
    select 'target warehouse has live auto_suspend' as check_name,
           (select count(*) from config where warehouse_name = upper('{{ target.warehouse }}')
              and auto_suspend_seconds is not null and scaling_policy = 'STANDARD') as produced,
           1 as expected
    union all
    select 'fixture warehouses have no live settings',
           (select count(*) from config where startswith(warehouse_name, 'FIXTURE_WH_') and auto_suspend_seconds is not null),
           0
    union all
    select 'FIXTURE_WH_IDLE auto-suspend cycles',
           (select max(autosuspend_cycles_30d) from {{ ref('int_snowflake__warehouse_suspend_cycles') }}
            where warehouse_name = 'FIXTURE_WH_IDLE'),
           30
)

select * from checks where produced is distinct from expected
