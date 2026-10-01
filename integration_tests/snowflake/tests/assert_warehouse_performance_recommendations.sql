{#-
  fct_snowflake__warehouse_performance_recommendations: per-table spillage tiers.
  It needs table-level attribution (ACCESS_HISTORY), so on Standard edition it must
  return no rows. On Enterprise edition, the seven spilling demo tables (fixture query
  history) get one tier each:
    - DEMO_SPILL_REMOTE      (IDLE, Small):     2 GB remote spill      → scale up to Medium
    - DEMO_SPILL_HEAVY_SMALL (COLD, Small):     60 GB local            → scale up to Medium
    - DEMO_SPILL_HEAVY_LARGE (BUSY_2XL):        60 GB local on 2X-Large → optimize SQL, no DDL
    - DEMO_SPILL_WORSENING   (IDLE):            49 GB local, recent    → monitor, trending worse
    - DEMO_SPILL_STEADY      (IDLE):            2 GB 20 days ago + 2 GB 5 days ago → monitor, stable
    - DEMO_SPILL_MINOR       (HEALTHY):         0.5 GB                 → stable, minor
    - DEMO_CHAIN_TABLE       (HEALTHY):         3 GB 20 days ago       → monitor, stable (view chain slice)
  Returns rows only on mismatch.
-#}
with produced as (
    select lower(table_name) as table_name, warehouse_name, recommendation_key, recommendation, snowflake_ddl
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }}
),

expected as (
{%- if var('snowflake_enterprise_edition', true) %}
    select 'demo_spill_remote' as table_name, 'FIXTURE_WH_IDLE' as warehouse_name,
           'remote_spill' as recommendation_key,
           'Scale up warehouse (remote spillage detected)' as recommendation,
           'ALTER WAREHOUSE FIXTURE_WH_IDLE SET WAREHOUSE_SIZE = ''MEDIUM'';' as snowflake_ddl
    union all select 'demo_spill_heavy_small', 'FIXTURE_WH_COLD', 'local_heavy_small_wh', 'Scale up warehouse (heavy local spillage on SMALL)',
           'ALTER WAREHOUSE FIXTURE_WH_COLD SET WAREHOUSE_SIZE = ''MEDIUM'';'
    union all select 'demo_spill_heavy_large', 'FIXTURE_WH_BUSY_2XL', 'local_heavy_large_wh', 'Optimize SQL (heavy spillage on large warehouse)', null
    union all select 'demo_spill_worsening',   'FIXTURE_WH_IDLE',     'local_moderate_worsening', 'Monitor — moderate spillage trending worse', null
    union all select 'demo_spill_steady',      'FIXTURE_WH_IDLE',     'local_moderate_stable', 'Monitor — moderate spillage (stable)', null
    union all select 'demo_spill_minor',       'FIXTURE_WH_HEALTHY',  'local_minor', 'Stable — minor spillage', null
    union all select 'demo_chain_table',       'FIXTURE_WH_HEALTHY',  'local_moderate_stable', 'Monitor — moderate spillage (stable)', null
{%- else %}
    select null::varchar as table_name, null::varchar as warehouse_name, null::varchar as recommendation_key,
           null::varchar as recommendation,
           null::varchar as snowflake_ddl
    where false
{%- endif %}
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.warehouse_name as produced_warehouse, e.warehouse_name as expected_warehouse,
    p.recommendation_key as produced_key,   e.recommendation_key as expected_key,
    p.recommendation as produced_rec,       e.recommendation as expected_rec,
    p.snowflake_ddl  as produced_ddl,       e.snowflake_ddl  as expected_ddl
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.warehouse_name is distinct from e.warehouse_name
   or p.recommendation_key is distinct from e.recommendation_key
   or p.recommendation is distinct from e.recommendation
   or p.snowflake_ddl  is distinct from e.snowflake_ddl
