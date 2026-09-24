{{
  config(
    materialized='table',
    post_hook="{{ probe_unique_key_candidates() }}"
  )
}}

{#--
  Redshift port of fct_snowflake__incremental_config_recommendations.

  Model 2 of the incremental materialization recommendation flow.
  Depends on fct_redshift__incremental_materialization_candidates (Model 1).

  Adds: Redshift-optimized incremental strategy recommendation, filter column
  detection, ranked unique key candidates, uniqueness validation SQL, and a
  copy-pasteable dbt config template with the is_incremental() filter block.

  Excludes 'Low ROI — Minimal Rebuild Redundancy' tables from Model 1.

  Strategy selection logic (evaluated in priority order):
    1. External deletes detected + filter column      → delete+insert (flagged for review)
    2. External deletes detected, no filter column    → merge (flagged for review)
    3. Unique key + filter + large scale              → delete+insert
    4. Unique key + filter, moderate scale            → merge
    5. Filter column only, no external DML, large     → (append — microbatch not in Redshift)
    6. Filter column only, no external DML            → append
    7. Filter column present (external DML, no key)  → append
    8. Unique key only, no filter column              → merge
    9. No key candidates                              → append

  Note: microbatch is a dbt Core 1.9+ Snowflake-specific strategy. Redshift
  uses 'append' in those cases instead.

  Unique key candidates are confirmed post-build via the probe_unique_key_candidates()
  post-hook, which uses APPROXIMATE COUNT(DISTINCT ...) to verify cardinality.

  Controlled by the following dbt variables:
    - incremental_large_table_row_threshold (default 10000000)
    - incremental_large_table_gb_threshold  (default 10)
    - incremental_unique_key_probe_threshold (default 0.95, used by post-hook)
--#}

{% set large_row_threshold = var('incremental_large_table_row_threshold', 10000000) %}
{% set large_gb_threshold  = var('incremental_large_table_gb_threshold', 10) %}

with candidates as (

    select *
    from {{ ref('fct_redshift__incremental_materialization_candidates') }}
    where recommendation != 'Low ROI — Minimal Rebuild Redundancy'

),

candidate_filter_keys as (

    -- timestamp/date columns ranked by name pattern suitability for is_incremental() filter
    select
        upper(c.table_fqn)                                                  as table_fqn,
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

    from {{ ref('int_redshift__table_columns') }} as c
    inner join candidates as ca
        on upper(c.table_fqn) = ca.table_fqn
    where c.data_type ilike 'TIMESTAMP%'
       or lower(c.data_type) = 'date'

),

candidate_unique_keys as (

    -- columns that are plausible unique key candidates by naming convention.
    -- excludes timestamp/date/float/boolean types. uniqueness is verified post-build
    -- by probe_unique_key_candidates() — use likely_unique_key for the confirmed result.
    select
        upper(c.table_fqn)                                                  as table_fqn,
        c.column_name,
        c.data_type,
        row_number() over (
            partition by c.table_fqn
            order by
                case
                    when lower(c.column_name) in ('surrogate_key', 'primary_key') then 1
                    when right(lower(c.column_name), 3) = '_sk'                   then 2
                    when lower(c.column_name) = 'id'                              then 3
                    when right(lower(c.column_name), 3) = '_id'                   then 4
                    when right(lower(c.column_name), 4) = '_key'                  then 5
                    else 6
                end,
                case
                    when c.data_type ilike 'INT%'
                      or c.data_type ilike 'BIGINT%'
                      or c.data_type ilike 'SMALLINT%'
                      or c.data_type ilike 'NUMERIC%'
                      or c.data_type ilike 'DECIMAL%'
                      or c.data_type ilike 'VARCHAR%'
                      or c.data_type ilike 'CHARACTER%'
                      or c.data_type ilike 'TEXT%'
                      or c.data_type ilike 'BPCHAR%' then 0
                    else 1
                end,
                c.ordinal_position
        )                                                                   as rn

    from {{ ref('int_redshift__table_columns') }} as c
    inner join candidates as ca
        on upper(c.table_fqn) = ca.table_fqn
    where (
            lower(c.column_name) in ('id', 'surrogate_key', 'primary_key')
            or right(lower(c.column_name), 3) = '_id'
            or right(lower(c.column_name), 4) = '_key'
            or right(lower(c.column_name), 3) = '_sk'
    )
    and c.data_type not ilike 'TIMESTAMP%'
    and c.data_type not ilike 'DATE%'
    and c.data_type not ilike 'FLOAT%'
    and c.data_type not ilike 'DOUBLE%'
    and c.data_type not ilike 'BOOL%'

),

best_filter_key as (

    select
        table_fqn,
        column_name as filter_column,
        data_type   as filter_column_type

    from candidate_filter_keys
    where rn = 1

),

top_unique_keys as (

    select
        table_fqn,
        json_parse(
            '[' ||
            listagg('"' || column_name || '"', ',')
                within group (order by rn)
            || ']'
        )                                                                   as unique_key_candidates,
        min(case when rn = 1 then column_name end)                         as best_unique_key

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
        (
            ca.total_rows > {{ large_row_threshold }}
            or ca.table_size_gb > {{ large_gb_threshold }}
        )                                                                   as is_large_table,
        case
            when ca.delete_count > 0 and bfk.filter_column is not null
                then 'delete+insert'
            when ca.delete_count > 0
                then 'merge'
            when tuk.best_unique_key is not null
                and bfk.filter_column is not null
                and (ca.total_rows > {{ large_row_threshold }} or ca.table_size_gb > {{ large_gb_threshold }})
                then 'delete+insert'
            when tuk.best_unique_key is not null and bfk.filter_column is not null
                then 'merge'
            -- Redshift: no microbatch support — fall through to append
            when bfk.filter_column is not null and ca.dml_count = 0
                then 'append'
            when bfk.filter_column is not null
                then 'append'
            when tuk.best_unique_key is not null
                then 'merge'
            else 'append'
        end                                                                 as incremental_strategy

    from candidates as ca
    left join best_filter_key as bfk
        on bfk.table_fqn = ca.table_fqn
    left join top_unique_keys as tuk
        on tuk.table_fqn = ca.table_fqn

)

select
    current_date                                                            as snapshot_date,
    getdate()                                                               as analyzed_at,
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
    -- dml breakdown
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
    null::varchar                                                           as likely_unique_key,
    -- strategy rationale
    -- Explicit varchar cast: Redshift infers a fixed CTAS column width from a
    -- single CASE branch, which can be too narrow for other branches' actual
    -- data-dependent lengths (real table/column names) — "value too long for
    -- type character varying(N)" (SQLSTATE 22001) at insert time otherwise.
    cast(
        case
            when incremental_strategy = 'delete+insert' and has_external_deletes
                then 'External deletes detected — delete+insert scoped to '
                    || suggested_filter_column
                    || ' filter window; records deleted outside the window require a periodic full-refresh to propagate'
            when incremental_strategy = 'delete+insert'
                then 'Large table ('
                    || table_size_gb::varchar || ' GB / ' || total_rows::varchar || ' rows'
                    || ') — delete+insert scoped to ' || suggested_filter_column
                    || ' window avoids full-target merge scan'
            when incremental_strategy = 'merge' and has_external_deletes
                then 'External deletes detected, no filter column available — merge on '
                    || coalesce(best_unique_key, 'unique key')
                    || ' is the safest option; add a timestamp column to enable delete+insert instead'
            when incremental_strategy = 'merge' and has_filter_column
                then 'Unique key candidate detected — merge on '
                    || best_unique_key
                    || ' scoped to ' || suggested_filter_column || ' filter window'
            when incremental_strategy = 'merge'
                then 'Unique key candidate (' || best_unique_key
                    || ') but no filter column found — merge scans the full target table on each run;'
                    || ' adding a timestamp column would enable a scoped delete+insert'
            when incremental_strategy = 'append' and has_filter_column and has_external_dml
                then 'External DML detected but no unique key candidate found — append with '
                    || suggested_filter_column
                    || ' filter is the starting point; if records are updated after insert,'
                    || ' identify a unique key and switch to merge to prevent duplicates'
            when incremental_strategy = 'append' and has_filter_column
                then 'No external DML detected — append inserts new rows only; '
                    || suggested_filter_column
                    || ' scopes the load window; verify data is truly append-only before implementing'
            when incremental_strategy = 'append' and has_unique_key_candidate
                then 'No filter column detected — append is the safest default; unique key candidate '
                    || best_unique_key
                    || ' is available if merge is needed; add a timestamp column to scope incremental loads'
            else
                'No filter column or unique key candidate detected — append is the safest default;'
                    || ' manual key identification required before implementing'
        end
    as varchar(2000))                                                       as strategy_notes,
    -- run this before implementing to verify the unique key candidate
    -- Explicit varchar cast: Redshift infers a fixed CTAS column width from a
    -- single CASE branch, which can be too narrow for other branches' actual
    -- data-dependent lengths (real table/column names) — "value too long for
    -- type character varying(N)" (SQLSTATE 22001) at insert time otherwise.
    cast(
        case
            when best_unique_key is not null
                then 'select count(*) = count(distinct '
                    || lower(best_unique_key)
                    || ') as is_unique from '
                    || lower(table_fqn)
        end
    as varchar(1000))                                                       as validate_uniqueness_sql,
    -- copy-pasteable dbt config template
    -- Explicit varchar cast: same Redshift implicit-CTAS-width issue as
    -- strategy_notes/validate_uniqueness_sql above — this is the longest and
    -- most branch-length-variable column in the mart, so it needs the most
    -- headroom.
    cast(
        case
            when incremental_strategy in ('merge', 'delete+insert')
                and suggested_filter_column is not null
                then
                    '{' || '{' || chr(10)
                    || '  config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''' || incremental_strategy || ''',' || chr(10)
                    || '    unique_key=''' || lower(coalesce(best_unique_key, '<unique_key>')) || ''',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || '  )' || chr(10)
                    || '}' || '}' || chr(10) || chr(10)
                    || '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || 'where ' || lower(suggested_filter_column)
                    || ' > (select max(' || lower(suggested_filter_column) || ') from '
                    || '{' || '{' || ' this ' || '}' || '}' || ')' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            when incremental_strategy in ('merge', 'delete+insert')
                then
                    '{' || '{' || chr(10)
                    || '  config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''' || incremental_strategy || ''',' || chr(10)
                    || '    unique_key=''' || lower(coalesce(best_unique_key, '<unique_key>')) || ''',' || chr(10)
                    || '    on_schema_change=''append_new_columns''' || chr(10)
                    || '  )' || chr(10)
                    || '}' || '}' || chr(10) || chr(10)
                    || '-- TODO: add a filter column (timestamp/date) to scope incremental loads'
            when incremental_strategy = 'append' and suggested_filter_column is not null
                then
                    '{' || '{' || chr(10)
                    || '  config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''append''' || chr(10)
                    || '  )' || chr(10)
                    || '}' || '}' || chr(10) || chr(10)
                    || '{' || '%' || ' if is_incremental() ' || '%' || '}' || chr(10)
                    || 'where ' || lower(suggested_filter_column)
                    || ' > (select max(' || lower(suggested_filter_column) || ') from '
                    || '{' || '{' || ' this ' || '}' || '}' || ')' || chr(10)
                    || '{' || '%' || ' endif ' || '%' || '}'
            else
                    '{' || '{' || chr(10)
                    || '  config(' || chr(10)
                    || '    materialized=''incremental'',' || chr(10)
                    || '    incremental_strategy=''append''' || chr(10)
                    || '  )' || chr(10)
                    || '}' || '}' || chr(10) || chr(10)
                    || '-- TODO: add a filter column (timestamp/date) to scope incremental loads' || chr(10)
                    || '-- TODO: verify data is truly append-only before using this strategy'
        end
    as varchar(4000))                                                       as dbt_config_template

from strategy_labeled
order by
    case when est_daily_redundant_gb_scanned is not null then 0 else 1 end,
    coalesce(est_daily_redundant_gb_scanned, 0) desc,
    compute_waste_score desc
