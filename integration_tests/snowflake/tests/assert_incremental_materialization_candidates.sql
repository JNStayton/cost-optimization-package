{#-
  fct_snowflake__incremental_materialization_candidates classifies the demo tables from
  their fixture build history (daily CTAS row counts):
    - demo_orders, demo_sessions, demo_logs: 14 builds, ~99.5% unchanged rows → Strong Candidate
    - demo_fast_growth: rows triple each build → Low ROI
    - demo_new_table: 2 builds → Insufficient History, ROI low (too little history to rate)
    - demo_infrequent_builds: 10 builds → Strong Candidate
  demo_events has no builds, so it isn't listed. Returns rows only on mismatch.
-#}
with produced as (
    select lower(table_name) as table_name, recommendation, roi_tier, table_build_count, qualified_build_days
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }}
    where startswith(lower(table_name), 'demo_')
),

expected as (
    select 'demo_orders' as table_name, 'Strong Candidate' as recommendation, 'high' as roi_tier,
           14 as table_build_count, 14 as qualified_build_days
    union all select 'demo_sessions',    'Strong Candidate',                     'high',   14, 14
    union all select 'demo_logs',        'Strong Candidate',                     'high',   14, 14
    union all select 'demo_fast_growth', 'Low ROI — Minimal Rebuild Redundancy', 'low',    14, 14
    union all select 'demo_new_table',   'Candidate — Insufficient History',     'low',     2,  2
    union all select 'demo_infrequent_builds', 'Strong Candidate',               'high',   10, 10
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.recommendation       as produced_recommendation, e.recommendation       as expected_recommendation,
    p.roi_tier             as produced_roi_tier,       e.roi_tier             as expected_roi_tier,
    p.table_build_count    as produced_builds,         e.table_build_count    as expected_builds,
    p.qualified_build_days as produced_build_days,     e.qualified_build_days as expected_build_days
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.recommendation       is distinct from e.recommendation
   or p.roi_tier             is distinct from e.roi_tier
   or p.table_build_count    is distinct from e.table_build_count
   or p.qualified_build_days is distinct from e.qualified_build_days
