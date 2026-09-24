{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='operator_evidence_key',
    cluster_by=['access_date'],
  )
}}

{#--
  Unified operator evidence accumulator from GET_QUERY_OPERATOR_STATS.
  Captures three operator types per query:
    - TableScan: exact per-table partition pruning evidence
    - Filter: column appeared in a WHERE/HAVING condition
    - Join: column appeared in a JOIN equality condition

  This table is populated by the extract_operator_evidence macro
  (run via post-hook on fct_snowflake__table_clustering_candidates).
  The model SQL only defines the schema and handles full-refresh resets.

  GET_QUERY_OPERATOR_STATS has 14-day retention, so this table accumulates
  history beyond that window. The macro handles deduplication on insert.

  Grain: one row per (query_id, operator type, table_fqn, column_name)
         For TableScan: column_name is NULL, partitions populated
         For Filter/Join: partitions are NULL, column_name populated
--#}

{% if is_incremental() %}
    select
        cast(null as varchar) as operator_evidence_key,
        cast(null as varchar) as query_id,
        cast(null as varchar) as table_fqn,
        cast(null as varchar) as operator_type,
        cast(null as varchar) as column_name,
        cast(null as int) as partitions_scanned,
        cast(null as int) as partitions_total,
        cast(null as bigint) as bytes_scanned,
        cast(null as varchar) as condition_text,
        cast(null as varchar) as query_parameterized_hash,
        cast(null as timestamp_ntz) as query_start_time,
        cast(null as date) as access_date
    where 1 = 0
{% else %}
    select
        cast(null as varchar) as operator_evidence_key,
        cast(null as varchar) as query_id,
        cast(null as varchar) as table_fqn,
        cast(null as varchar) as operator_type,
        cast(null as varchar) as column_name,
        cast(null as int) as partitions_scanned,
        cast(null as int) as partitions_total,
        cast(null as bigint) as bytes_scanned,
        cast(null as varchar) as condition_text,
        cast(null as varchar) as query_parameterized_hash,
        cast(null as timestamp_ntz) as query_start_time,
        cast(null as date) as access_date
    where 1 = 0
{% endif %}
