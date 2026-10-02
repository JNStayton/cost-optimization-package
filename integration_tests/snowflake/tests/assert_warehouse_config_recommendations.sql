{#-
  fct_snowflake__warehouse_config_recommendations gives each fixture
  warehouse the recommendation its metrics call for (macros/demo_warehouse_catalog.sql):
    - IDLE (50% idle credits, auto_suspend 300s)    → reduce auto-suspend to 60s
    - BUSY (Medium, 40% of elapsed time queued)     → scale up to Large
    - BUSY_XS (X-Small, queued)                     → scale up to Small; X-Small is the
                                                      smallest size, not the largest
    - BUSY_2XL (2X-Large, queued)                   → scale up to 3X-Large
    - BUSY_6XL (6X-Large, queued; the largest size)  → split workloads, no DDL
    - COLD (5 s median provisioning wait)           → move to Gen2
    - OVERSIZED (Large, 10% load, 0.1 s queries)    → scale down to Medium
    - HEALTHY, JOBS                                 → stable, no DDL
    - BUILD (the incremental slice's dbt builds; no metering or events) → stable, no DDL
    - SUSPENDED (X-Small, oversized metrics; latest event is an auto-suspend, which
      carries no size)                              → already at minimum, no DDL
  On Enterprise edition, the four queuing single-cluster warehouses (BUSY, BUSY_XS,
  BUSY_2XL, BUSY_6XL) get "enable multi-cluster" instead; the rest are the same.
  Multi-cluster branches (Enterprise; settings from the simulated SHOW WAREHOUSES hook,
  macros/simulate_show_warehouses.sql):
    - BURSTY (50% idle, auto-suspend 60, 90% load, single cluster) → 1.6 enable multi-cluster
    - MCW_IDLE (50% idle, auto-suspend 60, ECONOMY, 1–3 clusters)  → 1.2 switch to STANDARD
    - MCW_BUSY (queuing, STANDARD, 1–3 clusters)                    → 2.5 max clusters to 4
    - MCW_OVERSIZED (10% load, 1–2 clusters)                        → 5.1 disable multi-cluster
  On Standard edition they keep the defaults (300 s auto-suspend, one cluster): reduce
  auto-suspend (BURSTY, MCW_IDLE), scale up (MCW_BUSY), scale down (MCW_OVERSIZED).
  Returns rows only on mismatch.
-#}
with produced as (
    select warehouse_name, recommendation_key, snowflake_ddl
    from {{ ref('fct_snowflake__warehouse_config_recommendations') }}
    where startswith(warehouse_name, 'FIXTURE_WH_')
),

expected as (
    select 'FIXTURE_WH_IDLE' as warehouse_name, 'idle_reduce_auto_suspend' as recommendation_key,
           'ALTER WAREHOUSE FIXTURE_WH_IDLE SET AUTO_SUSPEND = 60;' as snowflake_ddl
{%- if var('snowflake_enterprise_edition', true) %}
    {#- Enterprise: a single-cluster warehouse with queuing gets multi-cluster first (2.1),
        whatever its size. #}
    {%- for wh in ['FIXTURE_WH_BUSY', 'FIXTURE_WH_BUSY_XS', 'FIXTURE_WH_BUSY_2XL', 'FIXTURE_WH_BUSY_6XL'] %}
    union all select '{{ wh }}', 'overload_enable_mcw',
        'ALTER WAREHOUSE {{ wh }} SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = ''STANDARD'';'
    {%- endfor %}
{%- else %}
    union all select 'FIXTURE_WH_BUSY',      'overload_scale_up_standard', 'ALTER WAREHOUSE FIXTURE_WH_BUSY SET WAREHOUSE_SIZE = ''LARGE'';'
    union all select 'FIXTURE_WH_BUSY_XS',   'overload_scale_up_standard', 'ALTER WAREHOUSE FIXTURE_WH_BUSY_XS SET WAREHOUSE_SIZE = ''SMALL'';'
    union all select 'FIXTURE_WH_BUSY_2XL',  'overload_scale_up_standard', 'ALTER WAREHOUSE FIXTURE_WH_BUSY_2XL SET WAREHOUSE_SIZE = ''3X-LARGE'';'
    union all select 'FIXTURE_WH_BUSY_6XL',  'overload_at_max_standard',   null
{%- endif %}
{%- if var('snowflake_enterprise_edition', true) %}
    union all select 'FIXTURE_WH_BURSTY',        'idle_enable_mcw_bursty',
        'ALTER WAREHOUSE FIXTURE_WH_BURSTY SET MAX_CLUSTER_COUNT = 2, MIN_CLUSTER_COUNT = 1, SCALING_POLICY = ''STANDARD'';'
    union all select 'FIXTURE_WH_MCW_IDLE',      'idle_switch_scaling_policy', 'ALTER WAREHOUSE FIXTURE_WH_MCW_IDLE SET SCALING_POLICY = ''STANDARD'';'
    union all select 'FIXTURE_WH_MCW_BUSY',      'overload_increase_clusters', 'ALTER WAREHOUSE FIXTURE_WH_MCW_BUSY SET MAX_CLUSTER_COUNT = 4;'
    union all select 'FIXTURE_WH_MCW_OVERSIZED', 'oversized_disable_mcw',      'ALTER WAREHOUSE FIXTURE_WH_MCW_OVERSIZED SET MAX_CLUSTER_COUNT = 1;'
{%- else %}
    union all select 'FIXTURE_WH_BURSTY',        'idle_reduce_auto_suspend',   'ALTER WAREHOUSE FIXTURE_WH_BURSTY SET AUTO_SUSPEND = 60;'
    union all select 'FIXTURE_WH_MCW_IDLE',      'idle_reduce_auto_suspend',   'ALTER WAREHOUSE FIXTURE_WH_MCW_IDLE SET AUTO_SUSPEND = 60;'
    union all select 'FIXTURE_WH_MCW_BUSY',      'overload_scale_up_standard', 'ALTER WAREHOUSE FIXTURE_WH_MCW_BUSY SET WAREHOUSE_SIZE = ''LARGE'';'
    union all select 'FIXTURE_WH_MCW_OVERSIZED', 'oversized_scale_down',       'ALTER WAREHOUSE FIXTURE_WH_MCW_OVERSIZED SET WAREHOUSE_SIZE = ''MEDIUM'';'
{%- endif %}
    union all select 'FIXTURE_WH_COLD',      'provisioning_gen2',          'ALTER WAREHOUSE FIXTURE_WH_COLD SET RESOURCE_CONSTRAINT = ''STANDARD_GEN_2'';'
    union all select 'FIXTURE_WH_OVERSIZED', 'oversized_scale_down',       'ALTER WAREHOUSE FIXTURE_WH_OVERSIZED SET WAREHOUSE_SIZE = ''MEDIUM'';'
    union all select 'FIXTURE_WH_HEALTHY',   'stable',                     null
    union all select 'FIXTURE_WH_JOBS',      'stable',                     null
    union all select 'FIXTURE_WH_BUILD',     'stable',                     null
    union all select 'FIXTURE_WH_SUSPENDED', 'oversized_at_minimum',       null
)

select
    coalesce(p.warehouse_name, e.warehouse_name) as warehouse_name,
    p.recommendation_key as produced_key, e.recommendation_key as expected_key,
    p.snowflake_ddl      as produced_ddl, e.snowflake_ddl      as expected_ddl
from produced as p
full outer join expected as e on p.warehouse_name = e.warehouse_name
where p.recommendation_key is distinct from e.recommendation_key
   or p.snowflake_ddl      is distinct from e.snowflake_ddl
