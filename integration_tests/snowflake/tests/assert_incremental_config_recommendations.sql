{#-
  fct_snowflake__incremental_config_recommendations proposes a strategy per candidate, and
  its post-hook probe_unique_key_candidates checks the proposed key on the real table:
    - demo_orders:   merge on UPDATED_AT; the probe confirms ORDER_ID → confidence 60 → 70
    - demo_sessions: merge; SESSION_ID repeats, so the probe fails → confidence 60 → 30,
                     'investigate', blocking signal key_not_exact_or_nullable
    - demo_logs:     no key-like column → append on LOGGED_AT, confidence 70
    - demo_infrequent_builds: merge; low build frequency starts confidence at 50
                     ('investigate'); the probe confirms RECORD_ID → 60, and the status is
                     recomputed to 'actionable_review'
  effort_category must follow the final status (the probe updates both).
    - demo_fast_growth (Low ROI) and demo_new_table (Insufficient History) aren't listed.
  Returns rows only on mismatch.
-#}
with produced as (
    select
        lower(table_name) as table_name, incremental_strategy, upper(suggested_filter_column) as filter_column,
        likely_unique_key, confidence_score, recommendation_status, effort_category,
        array_to_string(array_sort(blocking_signals), ',') as blocking_signals
    from {{ ref('fct_snowflake__incremental_config_recommendations') }}
    where startswith(lower(table_name), 'demo_')
),

expected as (
    select 'demo_orders' as table_name, 'merge' as incremental_strategy, 'UPDATED_AT' as filter_column,
           'order_id' as likely_unique_key, 70 as confidence_score, 'actionable_review' as recommendation_status,
           'actionable_review' as effort_category, '' as blocking_signals
    union all select 'demo_sessions', 'merge',  'STARTED_AT', null, 30, 'investigate', 'investigation', 'key_not_exact_or_nullable'
    union all select 'demo_logs',     'append', 'LOGGED_AT',  null, 70, 'actionable_review', 'actionable_review', ''
    union all select 'demo_infrequent_builds', 'merge', 'UPDATED_AT', 'record_id', 60, 'actionable_review', 'actionable_review', 'low_build_frequency'
)

select
    coalesce(p.table_name, e.table_name) as table_name,
    p.incremental_strategy  as produced_strategy,   e.incremental_strategy  as expected_strategy,
    p.filter_column         as produced_filter,     e.filter_column         as expected_filter,
    p.likely_unique_key     as produced_key,        e.likely_unique_key     as expected_key,
    p.confidence_score      as produced_confidence, e.confidence_score      as expected_confidence,
    p.recommendation_status as produced_status,     e.recommendation_status as expected_status,
    p.effort_category       as produced_effort,     e.effort_category       as expected_effort,
    p.blocking_signals      as produced_blocking,   e.blocking_signals      as expected_blocking
from produced as p
full outer join expected as e on p.table_name = e.table_name
where p.incremental_strategy  is distinct from e.incremental_strategy
   or p.filter_column         is distinct from e.filter_column
   or p.likely_unique_key     is distinct from e.likely_unique_key
   or p.confidence_score      is distinct from e.confidence_score
   or p.recommendation_status is distinct from e.recommendation_status
   or p.effort_category       is distinct from e.effort_category
   or p.blocking_signals      is distinct from e.blocking_signals
