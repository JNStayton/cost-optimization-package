{#-
  fct_snowflake__table_clustering_candidates flags demo_events, which the fixtures describe
  as read-heavy (20 reads, 2 writes) with poor pruning (90% of partitions scanned), as a
  Moderate impact candidate whose consumption queries justify evaluating clustering.
  The incremental slice's demo tables have builds but no reads, so they're listed as
  "No read activity" and aren't candidates. DEMO_EVENTS has 100 micropartitions: the
  query-weighted median of partitions per query from the pruning fixture's totals.
  Checks the latest snapshot only (the integration project keeps a stale one beside it).
  Returns rows only on mismatch.
-#}
with produced as (
    select lower(table_name) as table_name, is_candidate, recommendation_tier, recommendation_status,
           select_count, dml_count, scan_ratio_pct, estimated_micropartitions
    from {{ ref('fct_snowflake__table_clustering_candidates') }}
    where startswith(lower(table_name), 'demo_')
      and snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__table_clustering_candidates') }})
),

expected as (
    select 'demo_events' as table_name, true as is_candidate, 'Moderate impact' as recommendation_tier,
           'evaluate_clustering' as recommendation_status, 20 as select_count, 2 as dml_count, 90.0 as scan_ratio_pct,
           100 as estimated_micropartitions
{%- for t in ['demo_orders', 'demo_sessions', 'demo_logs', 'demo_fast_growth', 'demo_new_table', 'demo_infrequent_builds'] %}
    union all select '{{ t }}', false, 'No read activity', 'insufficient_evidence', 0, 0, null, null
{%- endfor %}
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.is_candidate          as produced_is_candidate,  e.is_candidate          as expected_is_candidate,
    p.recommendation_tier   as produced_tier,          e.recommendation_tier   as expected_tier,
    p.recommendation_status as produced_status,        e.recommendation_status as expected_status,
    p.select_count          as produced_select_count,  e.select_count          as expected_select_count,
    p.dml_count             as produced_dml_count,     e.dml_count             as expected_dml_count,
    p.scan_ratio_pct        as produced_scan_ratio,    e.scan_ratio_pct        as expected_scan_ratio,
    p.estimated_micropartitions as produced_micropartitions, e.estimated_micropartitions as expected_micropartitions
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.is_candidate          is distinct from e.is_candidate
   or p.recommendation_tier   is distinct from e.recommendation_tier
   or p.recommendation_status is distinct from e.recommendation_status
   or p.select_count          is distinct from e.select_count
   or p.dml_count             is distinct from e.dml_count
   -- Scan ratio is checked only for the candidate (null in expected = not checked).
   or (e.scan_ratio_pct is not null and p.scan_ratio_pct is distinct from e.scan_ratio_pct)
   or (e.estimated_micropartitions is not null
       and p.estimated_micropartitions is distinct from e.estimated_micropartitions)
