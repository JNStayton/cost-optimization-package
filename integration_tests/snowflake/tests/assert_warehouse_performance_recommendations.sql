{#-
  fct_snowflake__warehouse_performance_recommendations: per-table spillage tiers.
  It needs table-level attribution (ACCESS_HISTORY), so on Standard edition it must
  return no rows. On Enterprise edition, the seven spilling demo tables (fixture query
  history) get one tier each, and spilling_execution_s, the runtime of their spilling
  queries (ACCESS_HISTORY attribution; DEMO_SPILL_STEADY's two builds, 30 + 36 s; the
  heavy tables' real spill-evidence queries add 60 s and 10 s):
    - DEMO_SPILL_REMOTE      (IDLE, Small):     2 GB remote spill      → scale up to Medium
    - DEMO_SPILL_HEAVY_SMALL (BUSY, Medium):    60 GB local            → scale up to Large
    - DEMO_SPILL_HEAVY_LARGE (BUSY_2XL):        60 GB local on 2X-Large → optimize SQL, no DDL
    - DEMO_SPILL_WORSENING   (IDLE):            49 GB local, recent    → monitor, trending worse
    - DEMO_SPILL_STEADY      (IDLE):            2 GB 20 days ago + 2 GB 5 days ago → monitor, stable
    - DEMO_SPILL_MINOR       (HEALTHY):         0.5 GB                 → stable, minor
    - DEMO_CHAIN_TABLE       (HEALTHY):         3 GB 20 days ago       → monitor, stable (view chain slice)
  Returns rows only on mismatch.
-#}
with produced as (
    select lower(table_name) as table_name, warehouse_name, recommendation_key, recommendation, snowflake_ddl,
           spilling_execution_s
    from {{ ref('fct_snowflake__warehouse_performance_recommendations') }}
),

expected as (
{%- if var('snowflake_enterprise_edition', true) %}
    select 'demo_spill_remote' as table_name, 'FIXTURE_WH_IDLE' as warehouse_name,
           'remote_spill' as recommendation_key,
           'Scale up warehouse (remote spillage detected)' as recommendation,
           'ALTER WAREHOUSE FIXTURE_WH_IDLE SET WAREHOUSE_SIZE = ''MEDIUM'';' as snowflake_ddl,
           120 as spilling_execution_s
    union all select 'demo_spill_heavy_small', 'FIXTURE_WH_BUSY', 'local_heavy_small_wh', 'Scale up warehouse (heavy local spillage on MEDIUM)',
           'ALTER WAREHOUSE FIXTURE_WH_BUSY SET WAREHOUSE_SIZE = ''LARGE'';', 250
    union all select 'demo_spill_heavy_large', 'FIXTURE_WH_BUSY_2XL', 'local_heavy_large_wh', 'Optimize SQL (heavy spillage on large warehouse)', null, 660
    union all select 'demo_spill_worsening',   'FIXTURE_WH_IDLE',     'local_moderate_worsening', 'Monitor — moderate spillage trending worse', null, 90
    union all select 'demo_spill_steady',      'FIXTURE_WH_IDLE',     'local_moderate_stable', 'Monitor — moderate spillage (stable)', null, 66
    union all select 'demo_spill_minor',       'FIXTURE_WH_HEALTHY',  'local_minor', 'Stable — minor spillage', null, 15
    union all select 'demo_chain_table',       'FIXTURE_WH_HEALTHY',  'local_moderate_stable', 'Monitor — moderate spillage (stable)', null, 45
{%- else %}
    select null::varchar as table_name, null::varchar as warehouse_name, null::varchar as recommendation_key,
           null::varchar as recommendation,
           null::varchar as snowflake_ddl, null::number as spilling_execution_s
    where false
{%- endif %}
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.warehouse_name as produced_warehouse, e.warehouse_name as expected_warehouse,
    p.recommendation_key as produced_key,   e.recommendation_key as expected_key,
    p.recommendation as produced_rec,       e.recommendation as expected_rec,
    p.snowflake_ddl  as produced_ddl,       e.snowflake_ddl  as expected_ddl,
    p.spilling_execution_s as produced_spilling_s, e.spilling_execution_s as expected_spilling_s
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.warehouse_name is distinct from e.warehouse_name
   or p.recommendation_key is distinct from e.recommendation_key
   or p.recommendation is distinct from e.recommendation
   or p.snowflake_ddl  is distinct from e.snowflake_ddl
   or p.spilling_execution_s is distinct from e.spilling_execution_s
