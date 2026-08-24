{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='snapshot_optimization_snapshot_key',
    enabled=(target.type == 'databricks')
  )
}}

{#--
  Daily snapshot of dbt SNAPSHOT-resource models evaluated for optimization
  opportunities. Surfaces three classes of recommendation:

    1. Pause snapshot       — snapshot has run repeatedly but no new SCD-2
                              versions were added in the lookback window.
    2. Reduce run frequency — productive_run_pct is low and total scan cost
                              is non-trivial; running less often would cut
                              compute without sacrificing change capture.
    3. Switch to timestamp  — snapshot uses 'check' strategy but its source
                              table has a column matching common updated_at
                              naming patterns; timestamp strategy avoids the
                              per-column comparison cost.

  Key metrics:
    - productive_run_pct       — % of days in the window where the snapshot
                                 ran AND added at least one new version.
    - mb_scanned_per_new_version — average MB scanned for each new SCD-2
                                   version inserted. High values quantify
                                   why a snapshot is wasteful.

  Controlled by the following dbt variables:
    - snapshot_candidates_lookback_days                (default 30)
    - snapshot_candidates_min_run_count                (default 3)
    - snapshot_candidates_min_runs_for_pause           (default 14)
    - snapshot_candidates_low_productivity_threshold   (default 0.25)
--#}

{% set lookback_days                     = var('snapshot_candidates_lookback_days', 30) %}
{% set min_run_count                     = var('snapshot_candidates_min_run_count', 3) %}
{% set min_runs_for_pause                = var('snapshot_candidates_min_runs_for_pause', 14) %}
{% set low_productivity_threshold        = var('snapshot_candidates_low_productivity_threshold', 0.25) %}

{#--
  Window threshold, expressed with dbt's cross-database macros so the change-
  activity logic below is portable to any adapter (dbt_valid_from / dbt_valid_to
  are dbt-standard SCD-2 columns present on every platform).
--#}
{% set window_start = dbt.dateadd('day', -1 * lookback_days, dbt.current_timestamp()) %}

{#--
  Resolve the physical relation for each enabled snapshot that actually exists in
  the warehouse. We read SCD-2 metadata (dbt_valid_from / dbt_valid_to) straight
  from these tables — the only reliable measure of change activity. Statement-type
  counts from query history do NOT work: dbt writes snapshots via MERGE, so
  INSERT/UPDATE counts are ~always zero and a MERGE fires on every run whether or
  not any row changed. Only snapshots whose table currently exists are probed, so
  a not-yet-built snapshot never breaks compilation.
--#}
{% set snapshot_relations = [] %}
{% if execute %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "snapshot") | list %}
        {% if node.config and node.config.enabled != false %}
            {% set rel = adapter.get_relation(
                database=node.database,
                schema=node.schema,
                identifier=(node.alias if node.alias else node.name)
            ) %}
            {% if rel is not none %}
                {% do snapshot_relations.append({'unique_id': node.unique_id, 'relation': rel}) %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endif %}

with snapshots as (
    select
        dbt_snapshot,
        snapshot_name,
        package_name,
        table_fqn,
        database_name,
        schema_name,
        table_name,
        strategy,
        updated_at,
        check_cols,
        unique_key,
        invalidate_hard_deletes,
        parent_models,
        downstream_model_count
    from {{ ref('int_dbt__snapshots') }}
),

snapshot_dbt_queries as (
    -- mirror of int_databricks__dbt_model_run_history but unfiltered by relations,
    -- so we keep snapshot runs (snapshots are filtered out of int_dbt__relations)
    select
        statement_id,
        start_time,
        cast(start_time as date)              as run_date,
        total_duration_ms                     as execution_time_ms,
        coalesce(read_bytes, 0)               as bytes_scanned,
        coalesce(produced_rows, 0)            as produced_rows,
        regexp_extract(statement_text, '"node_id":\\s*"([^"]+)"', 1) as node_id
    from {{ ref('stg_databricks__query_history') }}
    where statement_text ilike '%"app": "dbt"%'
      and execution_status = 'FINISHED'
      and statement_text is not null
      and start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),

snapshot_runs_by_day as (
    select
        sdq.node_id,
        sdq.run_date,
        count(*)                              as runs_on_day,
        sum(sdq.bytes_scanned)                as bytes_scanned_on_day,
        sum(sdq.execution_time_ms)            as execution_time_ms_on_day
    from snapshot_dbt_queries as sdq
    where sdq.node_id like 'snapshot.%'
    group by sdq.node_id, sdq.run_date
),

snapshot_run_history as (
    select
        node_id,
        sum(runs_on_day)                                              as run_count,
        sum(bytes_scanned_on_day)                                     as total_bytes_scanned,
        sum(bytes_scanned_on_day) / nullif(sum(runs_on_day), 0)       as avg_bytes_scanned_per_run,
        sum(execution_time_ms_on_day) / nullif(sum(runs_on_day), 0)   as avg_execution_time_ms_per_run,
        count(distinct run_date)                                      as run_days,
        min(run_date)                                                 as first_run_date,
        max(run_date)                                                 as last_run_date
    from snapshot_runs_by_day
    group by node_id
),

-- Change activity, read straight from each snapshot's SCD-2 metadata.
--   new_versions_in_window   — rows whose dbt_valid_from falls in the window
--                              (net-new records + new versions of changed rows).
--   productive_version_days  — distinct calendar dates a new version appeared;
--                              a run-day only "counts" when it produced change.
--   supersedes_in_window     — rows whose dbt_valid_to was set in the window
--                              (a prior version closed out by a newer one).
-- The where-clause prunes each scan to the window. Grain: one row per snapshot.
snapshot_scd2_activity as (
    {% if snapshot_relations | length > 0 %}
        {% for s in snapshot_relations %}
        select
            '{{ s.unique_id }}'                                              as dbt_snapshot,
            sum(case when dbt_valid_from >= {{ window_start }} then 1 else 0 end)
                                                                             as new_versions_in_window,
            count(distinct case when dbt_valid_from >= {{ window_start }}
                                then cast(dbt_valid_from as date) end)       as productive_version_days,
            sum(case when dbt_valid_to is not null and dbt_valid_to >= {{ window_start }}
                     then 1 else 0 end)                                      as supersedes_in_window,
            max(dbt_valid_from)                                              as last_new_version_at
        from {{ s.relation }}
        where dbt_valid_from >= {{ window_start }}
           or (dbt_valid_to is not null and dbt_valid_to >= {{ window_start }})
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        -- No enabled snapshot tables exist yet — empty shell so the model still compiles.
        select
            cast(null as string)    as dbt_snapshot,
            cast(0 as bigint)       as new_versions_in_window,
            cast(0 as bigint)       as productive_version_days,
            cast(0 as bigint)       as supersedes_in_window,
            cast(null as timestamp) as last_new_version_at
        where 1 = 0
    {% endif %}
),

-- Tier 2: look for an updated_at-like column on the snapshot's primary source.
-- Uses the first entry of parent_models as a heuristic for the source table.
snapshot_source_columns as (
    select
        sn.dbt_snapshot,
        upper(c.catalog_name) || '.' || upper(c.schema_name) || '.' || upper(c.table_name) as source_table_fqn,
        c.column_name,
        c.data_type,
        case
            when lower(c.column_name) in ('updated_at', 'last_updated_at', 'modified_at')   then 1
            when lower(c.column_name) in ('loaded_at', 'synced_at', 'ingested_at')          then 2
            when lower(c.column_name) in ('changed_at', 'last_modified_at', 'last_changed') then 3
            else 99
        end as rank
    from snapshots as sn
    inner join {{ ref('stg_databricks__columns') }} as c
        on upper(c.catalog_name) || '.' || upper(c.schema_name) || '.' || upper(c.table_name)
           = element_at(sn.parent_models, 1)
    where lower(c.data_type) in ('timestamp', 'timestamp_ntz', 'timestamp_ltz', 'date')
      and (
            lower(c.column_name) like '%updated_at%'
         or lower(c.column_name) like '%modified_at%'
         or lower(c.column_name) like '%loaded_at%'
         or lower(c.column_name) like '%ingested_at%'
         or lower(c.column_name) like '%synced_at%'
         or lower(c.column_name) like '%changed_at%'
      )
),

source_updated_at_suggestion as (
    select
        dbt_snapshot,
        min_by(column_name, rank) as suggested_updated_at_column,
        true                       as source_has_updated_at
    from snapshot_source_columns
    group by dbt_snapshot
),

assembled as (
    select
        sn.dbt_snapshot,
        sn.snapshot_name,
        sn.package_name,
        sn.table_fqn,
        sn.database_name,
        sn.schema_name,
        sn.table_name,
        sn.strategy,
        sn.updated_at,
        sn.check_cols,
        sn.unique_key,
        sn.invalidate_hard_deletes,
        sn.downstream_model_count,
        -- Run history
        coalesce(srh.run_count, 0)                                                       as run_count,
        round(coalesce(srh.total_bytes_scanned, 0) / power(1024, 3), 4)                  as total_bytes_scanned_gb,
        round(coalesce(srh.avg_bytes_scanned_per_run, 0) / power(1024, 3), 4)            as avg_bytes_scanned_per_run_gb,
        round(coalesce(srh.avg_execution_time_ms_per_run, 0) / 1000.0, 2)                as avg_execution_time_per_run_s,
        srh.first_run_date,
        srh.last_run_date,
        -- Change activity (from SCD-2 metadata — see snapshot_scd2_activity)
        coalesce(sca.new_versions_in_window, 0)                                          as total_new_versions,
        coalesce(sca.supersedes_in_window, 0)                                            as total_supersedes,
        sca.last_new_version_at,
        -- Productivity
        coalesce(srh.run_days, 0)                                                        as total_run_days,
        coalesce(sca.productive_version_days, 0)                                         as productive_run_days,
        round(
            coalesce(sca.productive_version_days, 0)
                / nullif(srh.run_days, 0)::double,
            4
        )                                                                                 as productive_run_pct,
        -- Scan-per-new-version
        round(
            coalesce(srh.total_bytes_scanned, 0) / 1024 / 1024
                / nullif(sca.new_versions_in_window, 0)::double,
            1
        )                                                                                 as mb_scanned_per_new_version,
        -- Tier 2 — strategy fit
        coalesce(sas.source_has_updated_at, false)                                       as source_has_updated_at,
        sas.suggested_updated_at_column,
        (
            lower(sn.strategy) = 'check'
            and coalesce(sas.source_has_updated_at, false) = true
        )                                                                                 as could_switch_to_timestamp
    from snapshots                            as sn
    left join snapshot_run_history            as srh on srh.node_id      = sn.dbt_snapshot
    left join snapshot_scd2_activity          as sca on sca.dbt_snapshot = sn.dbt_snapshot
    left join source_updated_at_suggestion    as sas on sas.dbt_snapshot = sn.dbt_snapshot
)

select
    current_date()                                                                       as snapshot_date,
    md5(
        cast(current_date() as string) || '|' || coalesce(dbt_snapshot, '')
    )                                                                                    as snapshot_optimization_snapshot_key,
    *,
    case
        when run_count >= {{ min_runs_for_pause }} and total_new_versions = 0
            then 'Pause snapshot'
        when run_count >= {{ min_run_count }}
            and productive_run_pct is not null
            and productive_run_pct < {{ low_productivity_threshold }}
            and total_bytes_scanned_gb >= 1
            then 'Reduce run frequency'
        when could_switch_to_timestamp
            then 'Switch to timestamp strategy'
        when run_count = 0
            then 'No runs in lookback window'
        else 'Healthy'
    end                                                                                  as recommendation,
    case
        when run_count >= {{ min_runs_for_pause }} and total_new_versions = 0
            then 'Snapshot ran ' || cast(run_count as string) || ' times in the last '
                || '{{ lookback_days }} days but no new SCD-2 versions were inserted. '
                || 'Pause or convert to a full-refresh table — incremental change capture is not delivering value.'
        when run_count >= {{ min_run_count }}
            and productive_run_pct is not null
            and productive_run_pct < {{ low_productivity_threshold }}
            and total_bytes_scanned_gb >= 1
            then 'Only ' || cast(productive_run_days as string)
                || ' of ' || cast(total_run_days as string)
                || ' run-days were productive ('
                || cast(round(productive_run_pct * 100, 1) as string)
                || '%) — '
                || cast(total_bytes_scanned_gb as string)
                || ' GB scanned total, '
                || cast(mb_scanned_per_new_version as string)
                || ' MB per new version. Reduce run frequency to align with the observed change rate.'
        when could_switch_to_timestamp
            then 'Source has timestamp column '
                || coalesce(suggested_updated_at_column, '?')
                || ' but snapshot uses check strategy. timestamp strategy avoids per-column comparison cost.'
        when run_count = 0
            then 'No dbt-attributed runs recorded in the last {{ lookback_days }} days.'
        else 'Snapshot is operating within expected efficiency bounds.'
    end                                                                                  as recommendation_reason,
    round(
        case
            when run_count >= {{ min_runs_for_pause }} and total_new_versions = 0
                then total_bytes_scanned_gb * 30.0 / nullif({{ lookback_days }}, 0)
            when run_count >= {{ min_run_count }}
                and productive_run_pct is not null
                and productive_run_pct < {{ low_productivity_threshold }}
                and total_bytes_scanned_gb >= 1
                then total_bytes_scanned_gb * (1 - productive_run_pct) * 30.0 / nullif({{ lookback_days }}, 0)
            else null
        end,
        2
    )                                                                                    as estimated_monthly_savings_gb
from assembled
{% if is_incremental() %}
-- current_date() is the value of snapshot_date for every row; reference the
-- function directly (a SELECT-list alias cannot be used in this WHERE clause).
where current_date() >= (
    select coalesce(max(snapshot_date), cast('1970-01-01' as date))
    from {{ this }}
)
{% endif %}
