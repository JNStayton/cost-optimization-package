{#-
  refresh_warehouse_config (post-hook on int_snowflake__warehouse_config) merges live
  SHOW WAREHOUSES settings. The fixture warehouses don't exist live, so they must keep null
  settings (the recommendations then fall back to 300 s auto-suspend), except the four the
  integration project's simulate_show_warehouses hook sets on Enterprise edition. The target's own
  warehouse does exist, so it must have its live size and auto-suspend, plus its live
  scaling policy and cluster counts on Enterprise edition (fixed STANDARD / 1 on Standard) (the fixtures have no events for it, so its size comes only from
  the hook). Every warehouse the hook gives a size must also get is_smallest_size and
  is_largest_size. Also checks the auto-suspend cycle count for FIXTURE_WH_IDLE.
  Returns one row per failed check.
-#}
-- depends_on: {{ ref('int_snowflake__warehouse_config') }}
with config as (
    select * from {{ ref('int_snowflake__warehouse_config') }}
),

checks as (
    select 'target warehouse has live auto_suspend' as check_name,
           (select count(*) from config where warehouse_name = upper('{{ target.warehouse }}')
              and current_size is not null and auto_suspend_seconds is not null
              {%- if var('snowflake_enterprise_edition', true) %}
              -- Enterprise: the warehouse's real scaling policy and cluster counts
              and scaling_policy is not null and max_cluster_count is not null
              {%- else %}
              -- Standard edition has no multi-cluster warehouses: fixed values
              and scaling_policy = 'STANDARD' and max_cluster_count = 1
              {%- endif %}
              ) as produced,
           1 as expected
    union all
    select 'fixture warehouses have no live settings',
           (select count(*) from config where startswith(warehouse_name, 'FIXTURE_WH_') and auto_suspend_seconds is not null
              and warehouse_name not in ('FIXTURE_WH_BURSTY', 'FIXTURE_WH_MCW_IDLE', 'FIXTURE_WH_MCW_BUSY', 'FIXTURE_WH_MCW_OVERSIZED')),
           0
    union all
    -- The simulated SHOW WAREHOUSES values (Enterprise only; macros/simulate_show_warehouses.sql),
    -- with a max cluster count above 1 marking the warehouse multi-cluster
    select 'simulated SHOW WAREHOUSES settings',
           (select count(*) from config
            where warehouse_name in ('FIXTURE_WH_BURSTY', 'FIXTURE_WH_MCW_IDLE', 'FIXTURE_WH_MCW_BUSY', 'FIXTURE_WH_MCW_OVERSIZED')
              and auto_suspend_seconds is not null
              and is_multicluster = (max_cluster_count > 1)
              and (warehouse_category = 'multi_cluster') = (max_cluster_count > 1)),
           {{ 4 if var('snowflake_enterprise_edition', true) else 0 }}
    union all
    select 'live sizes have size flags',
           (select count(*) from config where current_size is not null
              and (is_smallest_size is null or is_largest_size is null)),
           0
    union all
    select 'FIXTURE_WH_IDLE auto-suspend cycles',
           (select max(autosuspend_cycles_30d) from {{ ref('int_snowflake__warehouse_suspend_cycles') }}
            where warehouse_name = 'FIXTURE_WH_IDLE'),
           30
)

select * from checks where produced is distinct from expected
