{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='spill_evidence_key',
    on_schema_change='append_new_columns',
  )
}}

{#--
  Where spilling queries' time goes, from GET_QUERY_OPERATOR_STATS. One row per
  (sampled spilling query, table it's attributed to).

  Populated by the extract_spill_evidence post-hook on
  fct_snowflake__warehouse_performance_recommendations: for each spilling table, the
  most recent completed spilling query of each query shape in the last 14 days (the
  operator stats retention). dbt creates the table on first run (empty) and never
  inserts rows itself, so the hook's rows survive later runs.

  spill_blocked_s is the time the query's spilling operators spent blocked on disk:
  execution time x the sum of their local and remote disk I/O shares. Only operators
  with spilling statistics count; disk I/O elsewhere (cache reads, scans) isn't spill.
  It's a lower bound on what removing the spill saves.

  evidence_status is 'skipped' when the stats couldn't be read (the role needs to own
  the query or hold MONITOR on its warehouse, or the query is too old); skipped
  queries carry no times and are left out of the measurements.
--#}

select
    cast(null as varchar)       as spill_evidence_key,
    cast(null as varchar)       as query_id,
    cast(null as varchar)       as table_fqn,
    cast(null as varchar)       as query_parameterized_hash,
    cast(null as timestamp_ltz) as query_start_time,
    cast(null as varchar)       as warehouse_name,
    cast(null as varchar)       as warehouse_size,
    cast(null as float)         as execution_time_s,
    cast(null as int)           as spilling_operator_count,
    cast(null as float)         as spilling_operator_share,
    cast(null as float)         as blocked_on_disk_share,
    cast(null as float)         as spill_blocked_s,
    cast(null as bigint)        as bytes_spilled_local_ops,
    cast(null as bigint)        as bytes_spilled_remote_ops,
    cast(null as varchar)       as evidence_status,
    cast(null as varchar)       as skip_reason,
    cast(null as timestamp_ltz) as analyzed_at
where 1 = 0
