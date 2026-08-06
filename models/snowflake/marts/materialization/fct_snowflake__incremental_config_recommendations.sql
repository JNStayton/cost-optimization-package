{{
  config(
    materialized='table',
    post_hook="{{ probe_unique_key_candidates() }}"
  )
}}

{#--
  Model 2 of the incremental materialization recommendation flow.
  Depends on fct_snowflake__incremental_materialization_candidates (Model 1).

  Adds: Snowflake-optimized incremental strategy recommendation, filter column
  detection, ranked unique key candidates, uniqueness validation SQL, and a
  copy-pasteable dbt config template with the is_incremental() filter block.

  Excludes 'Low ROI — Minimal Rebuild Redundancy' tables from Model 1; those
  tables grow too quickly for incremental to save meaningfully.

  Strategy selection logic (evaluated in priority order):
    1. External deletes detected + filter column      → delete+insert (flagged for review)
    2. External deletes detected, no filter column    → merge (flagged for review)
    3. Unique key + filter + large scale              → delete+insert
    4. Unique key + filter, moderate scale            → merge
    5. Filter column only, no external DML, large     → microbatch
    6. Filter column only, no external DML            → append
    7. Filter column present (external DML, no key)  → append
    8. Unique key only, no filter column              → merge
    9. No key candidates                              → append

  Controlled by the following dbt variables:
    - incremental_large_table_row_threshold (default 10000000)
    - incremental_large_table_gb_threshold  (default 10)
--#}

{% set large_row_threshold = var('incremental_large_table_row_threshold', 10000000) %}
{% set large_gb_threshold  = var('incremental_large_table_gb_threshold', 10) %}

with candidates as (
    select *
    from {{ ref('fct_snowflake__incremental_materialization_candidates') }}
    where recommendation != 'Low ROI — Minimal Rebuild Redundancy'
),

candidate_filter_keys as (
    -- timestamp/date columns ranked by name pattern suitability for is_incremental() filter
    select
        c.table_fqn,
        c.column_name,
        c.data_type,
        row_number() over (
            partition by c.table_fqn
            order by
                case
                    when lower(c.column_name) ilike '%updated_at%'
                      or lower(c.column_name) ilike '%modified_at%'        then 1
                    when lower(c.column_name) ilike '%loaded_at%'
                      or lower(c.column_name) ilike '%ingested_at%'
                      or lower(c.column_name) ilike '%inserted_at%'
                      or lower(c.column_name) ilike '%synced_at%'          then 2
                    when lower(c.column_name) ilike '%created_at%'
                      or lower(c.column_name) ilike '%event_date%'
                      or lower(c.column_name) ilike '%event_time%'
                      or lower(c.column_name) ilike '%event_timestamp%'    then 3
                    else 4
                end,
                c.ordinal_position
        )                                                                   as rn
    from {{ ref('int_snowflake__table_columns') }}                          as c
    inner join candidates                                                   as ca
        on ca.table_fqn = c.table_fqn
    where c.data_type ilike 'TIMESTAMP%'
       or c.data_type ilike 'DATE'
),

candidate_unique_keys as (
    -- columns that are plausible unique key candidates by naming convention.
    -- excludes timestamp/date/float/boolean types. uniqueness is not verified here —
    -- use identified_unique_key in the output before implementing.
    select
        c.table_fqn,
        c.column_name,
        c.data_type,
        row_number() over (
            partition by c.table_fqn
            order by
                case
                    when lower(c.column_name) in ('surrogate_key', 'primary_key') then 1
                    when endswith(lower(c.column_name), '_sk')                     then 2
                    when lower(c.column_name) = 'id'                              then 3
                    when endswith(lower(c.column_name), '_id')                    then 4
                    when endswith(lower(c.column_name), '_key')                   then 5
                    else 6
                end,
                case
                    when c.data_type ilike 'NUMBER%'
                      or c.data_type ilike 'INT%'
                      or c.data_type ilike 'BIGINT%'
                      or c.data_type ilike 'TEXT%'
                      or c.data_type ilike 'VARCHAR%'
                      or c.data_type ilike 'STRING%' then 0
                    else 1
                end,
                c.ordinal_position
        )                                                                   as rn
    from {{ ref('int_snowflake__table_columns') }}                          as c
    inner join candidates                                                   as ca
        on ca.table_fqn = c.table_fqn
    where (
            lower(c.column_name) in ('id', 'surrogate_key', 'primary_key')
            or endswith(lower(c.column_name), '_id')
            or endswith(lower(c.column_name), '_key')
            or endswith(lower(c.column_name), '_sk')
    )
    and c.data_type not ilike 'TIMESTAMP%'
    and c.data_type not ilike 'DATE%'
    and c.data_type not ilike 'FLOAT%'
    and c.data_type not ilike 'DOUBLE%'
    and c.data_type not ilike 'BOOLEAN%'
),

best_filter_key as (
    select
        table_fqn,
        column_name  as filter_column,
        data_type    as filter_column_type
    from candidate_filter_keys
    where rn = 1
),

top_unique_keys as (
    select
        table_fqn,
        array_agg(column_name) within group (order by rn) as unique_key_candidates,
        min_by(column_name, rn)                            as best_unique_key
    from candidate_unique_keys
    where rn <= 3
    group by table_fqn
),

strategy_labeled as (
    select
        ca.*,
        bfk.filter_column                                                   as suggested_filter_column,
        tuk.unique_key_candidates,
        tuk.best_unique_key,
        bfk.filter_column is not null                                       as has_filter_column,
        tuk.best_unique_key is not null                                     as has_unique_key_candidate,
        ca.delete_count > 0                                                 as has_external_deletes,
        ca.dml_count > 0                                                    as has_external_dml,
        -- target DML evidence (risk signal, not source proof)
        ca.update_count > 0 or ca.merge_count > 0                           as has_target_mutations,
        ca.insert_count > 0
            and ca.update_count = 0
            and ca.delete_count = 0
            and ca.merge_count = 0                                          as is_insert_only_target,
        (
            ca.total_rows > {{ large_row_threshold }}
            or ca.table_size_gb > {{ large_gb_threshold }}
        )                                                                   as is_large_table,
        -- Strategy inference (by data-change semantics, not table size)
        case
            -- S.1: watermark + key + no deletes/mutations → merge
            when bfk.filter_column is not null
                and tuk.best_unique_key is not null
                and ca.delete_count = 0
                and ca.update_count = 0
                and ca.merge_count = 0
                then 'merge'
            -- S.2: watermark + INSERT-only → append
            when bfk.filter_column is not null
                and ca.dml_count = 0
                then 'append'
            -- S.3: watermark + key + deletes observed → merge (with delete warning)
            when bfk.filter_column is not null
                and tuk.best_unique_key is not null
                and ca.delete_count > 0
                then 'merge'
            -- S.4: watermark + INSERT-only but no key → append (weaker)
            when bfk.filter_column is not null
                and ca.delete_count = 0
                then 'append'
            -- S.5: no watermark → cannot propose strategy
            else null
        end                                                                 as incremental_strategy
    from candidates                                                         as ca
    left join best_filter_key                                               as bfk
        on bfk.table_fqn = ca.table_fqn
    left join top_unique_keys                                               as tuk
        on tuk.table_fqn = ca.table_fqn
),

confidence_scored as (
    select
        *,
        -- Base confidence: 100, then deductions and bonuses
        100
            -- Assumptions (inferred, not proven)
            + case when has_filter_column then -10 else 0 end               -- watermark inferred from naming
            + case when has_filter_column and incremental_strategy is not null then -10 else 0 end  -- time-local assumed
            + case when incremental_strategy is not null then -10 else 0 end  -- lookback assumed
            + case when is_insert_only_target and incremental_strategy = 'append' then -15 else 0 end  -- append-only inferred from DML
            + case when has_unique_key_candidate and incremental_strategy = 'merge' then -10 else 0 end  -- key inferred from naming (pre-probe)
            -- Blocking signals (observed risks)
            + case when has_external_deletes then -25 else 0 end            -- deletes without reconciliation
            + case when has_target_mutations and not has_unique_key_candidate then -20 else 0 end  -- mutations without key
            + case when builds_per_day < 0.2 then -10 else 0 end            -- low frequency
            -- Bonuses (validated later by post-hook; start without them)
            + case when is_insert_only_target and not has_external_deletes then 5 else 0 end  -- INSERT-only for 30+ days
        as confidence_score_raw,
        -- Cannot exceed 100 or go below 0
        greatest(0, least(100,
            100
            + case when has_filter_column then -10 else 0 end
            + case when has_filter_column and incremental_strategy is not null then -10 else 0 end
            + case when incremental_strategy is not null then -10 else 0 end
            + case when is_insert_only_target and incremental_strategy = 'append' then -15 else 0 end
            + case when has_unique_key_candidate and incremental_strategy = 'merge' then -10 else 0 end
            + case when has_external_deletes then -25 else 0 end
            + case when has_target_mutations and not has_unique_key_candidate then -20 else 0 end
            + case when builds_per_day < 0.2 then -10 else 0 end
            + case when is_insert_only_target and not has_external_deletes then 5 else 0 end
        )) as confidence_score,
        -- Assumptions array
        array_construct_compact(
            iff(has_filter_column and incremental_strategy is not null,
                suggested_filter_column || ' is a reliable change boundary', null),
            iff(incremental_strategy is not null,
                'Data is time-local (historical output does not change outside lookback)', null),
            iff(incremental_strategy is not null,
                'Configured lookback captures late-arriving records', null),
            iff(is_insert_only_target and incremental_strategy = 'append',
                'Source is append-only (INSERT-only target DML observed)', null),
            iff(has_unique_key_candidate and incremental_strategy = 'merge',
                best_unique_key || ' is unique and non-null (pending validation)', null)
        ) as assumptions,
        -- Blocking signals array (accumulated, not short-circuited)
        array_construct_compact(
            iff(not has_filter_column, 'missing_filter_column', null),
            iff(has_external_deletes, 'deletes_observed_without_reconciliation', null),
            iff(has_target_mutations and not has_unique_key_candidate, 'mutations_observed_without_key', null),
            iff(has_unique_key_candidate and incremental_strategy = 'merge', 'key_pending_exact_validation', null),
            iff(builds_per_day < 0.2, 'low_build_frequency', null)
        ) as blocking_signals
    from strategy_labeled
)

select
    current_date()                                                          as snapshot_date,
    current_timestamp()                                                     as analyzed_at,
    -- table identity
    table_fqn,
    database_name,
    schema_name,
    table_name,
    dbt_model,
    model_name,
    package_name,
    -- model 1 redundancy summary
    recommendation                                                          as redundancy_tier,
    recommendation_reason                                                   as redundancy_reason,
    rebuild_redundancy_rate,
    est_daily_redundant_gb_scanned,
    table_size_gb,
    total_rows,
    table_build_count,
    builds_per_day,
    max_build_time_sec,
    avg_build_time_sec,
    -- dml breakdown (target DML evidence — risk signal, not source proof)
    dml_count,
    insert_count,
    update_count,
    delete_count,
    merge_count,
    -- strategy recommendation
    incremental_strategy,
    has_filter_column,
    has_unique_key_candidate,
    has_external_deletes,
    is_large_table,
    -- key detection
    suggested_filter_column,
    unique_key_candidates,
    best_unique_key,
    -- populated post-build by probe_unique_key_candidates() — null until macro runs
    null::string                                                            as likely_unique_key,
    -- confidence system
    case when incremental_strategy is null then null else confidence_score end as confidence_score,
    case
        when roi_tier = 'low' then 'do_not_recommend'
        when incremental_strategy is null then 'investigate'
        when confidence_score >= 60 then 'actionable_review'
        when confidence_score >= 30 then 'investigate'
        else 'investigate'
    end                                                                     as recommendation_status,
    assumptions,
    blocking_signals,
    roi_tier,
    -- strategy rationale
    case
        when incremental_strategy is null
            then 'No timestamp/date column detected — cannot propose a bounded change set. '
                || 'Add a watermark column (e.g., _loaded_at, updated_at) to enable incremental.'
        when incremental_strategy = 'merge' and has_external_deletes
            then 'Merge on ' || coalesce(best_unique_key, '<key>') || ' scoped to '
                || suggested_filter_column || '. WARNING: target DELETEs observed — merge does NOT '
                || 'propagate source deletions. Rows deleted from source will remain in target unless '
                || 'a reconciliation mechanism is implemented.'
        when incremental_strategy = 'merge'
            then 'Merge on ' || coalesce(best_unique_key, '<key>') || ' scoped to '
                || suggested_filter_column || ' lookback window. Assumes upsert semantics (inserts + updates).'
        when incremental_strategy = 'append' and is_insert_only_target
            then 'Append new rows scoped to ' || suggested_filter_column
                || '. INSERT-only target DML observed (supporting evidence, not proof of source contract). '
                || 'Verify source is truly append-only and no late arrivals occur beyond the lookback window.'
        when incremental_strategy = 'append'
            then 'Append new rows scoped to ' || suggested_filter_column
                || '. No key candidate available — append is the proposed strategy. '
                || 'Duplicates will occur if source rows arrive more than once.'
        else 'Strategy inference failed — investigate manually.'
    end                                                                     as strategy_notes,
    -- Only relevant for merge strategies with a validated key
    case when incremental_strategy = 'merge' then best_unique_key end       as identified_unique_key,
    -- Config template: always generated when a strategy can be proposed (confidence gates label, not content)
    case
        when incremental_strategy is not null
            then {{ build_incremental_config_template() }}
        else null
    end                                                                     as dbt_model_config,
    -- Effort category: aligned with recommendation status
    case recommendation_status
        when 'actionable_review' then 'actionable_review'
        when 'investigate' then 'investigation'
        else 'investigation'
    end                                                                     as effort_category
from confidence_scored
where roi_tier != 'low'
order by
    case recommendation_status
        when 'actionable_review' then 1
        when 'investigate' then 2
        else 3
    end,
    confidence_score desc,
    coalesce(est_daily_redundant_gb_scanned, 0) desc,
    rebuild_pressure_score desc
