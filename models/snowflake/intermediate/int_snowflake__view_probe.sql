-- depends_on: {{ ref('int_snowflake__view_chain_pairs') }}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='view_fqn',
    on_schema_change='append_new_columns',
    post_hook="{{ probe_view_recompute() }}",
  )
}}

{#--
  Measured recompute cost of views that feed tables through a view chain: one row per
  view, the latest probe. A view's read duration is a poor guide to what a downstream
  table build pays for it (reads can be filtered, and a view nearer the table inlines
  every view above it), so the probe_view_recompute post-hook runs
  `select hash_agg(*) from <view>` with the result cache off and records the execution
  time. hash_agg(*) forces every column to be computed; count(*) would let Snowflake
  skip the column work.

  dbt creates the table on first run (empty) and never inserts rows itself: the
  incremental model returns 0 rows, so the hook's rows survive later runs.
  fct_snowflake__table_materialization_candidates reads it.

  Variables:
    table_materialization_view_probe_limit        (default 10) — most views probed per
                                                    run; 0 turns the probe off
    table_materialization_view_probe_refresh_days (default 7)  — re-probe a view after
                                                    this many days
--#}

select
    cast(null as varchar)       as view_fqn,
    cast(null as varchar)       as probe_query_id,
    cast(null as varchar)       as probe_status,
    cast(null as varchar)       as probe_error,
    cast(null as bigint)        as execution_time_ms,
    cast(null as bigint)        as total_elapsed_time_ms,
    cast(null as varchar)       as warehouse_name,
    cast(null as varchar)       as warehouse_size,
    cast(null as timestamp_ltz) as probed_at
where 1 = 0
