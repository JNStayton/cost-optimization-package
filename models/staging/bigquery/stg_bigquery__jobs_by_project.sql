{#--
  Upgrade note: referenced_tables/destination_table are new columns as of this change.
  on_schema_change='append_new_columns' handles the ALTER TABLE for existing installs, but
  rows merged in before the upgrade get NULL for both columns (dbt backfills new columns as
  NULL, it doesn't re-derive them). Downstream, int_bigquery__table_query_stats_daily's reads
  CTE does `cross join unnest(referenced_tables)`, and UNNEST(NULL) yields zero rows - so
  pre-upgrade jobs silently drop out of read attribution rather than erroring. Run with
  --full-refresh on upgrade (this model and int_bigquery__table_query_stats_daily) to
  re-derive historical attribution from JOBS_BY_PROJECT's own history instead.
--#}
{{ config(
    materialized='incremental',
    unique_key='job_id',
    on_schema_change='append_new_columns'
) }}

select
    job_id,
    creation_time,
    start_time,
    end_time,
    total_slot_ms,
    total_bytes_processed,
    total_bytes_billed,
    cache_hit,
    query,
    statement_type,
    user_email,
    state,
    error_result,
    reservation_id,
    bi_engine_statistics,
    referenced_tables,
    destination_table
from {{ source('bigquery_region_info', 'JOBS_BY_PROJECT') }}

{% if is_incremental() %}
  where creation_time >= (select timestamp_sub(max(creation_time), interval 7 day) from {{ this }})
{% endif %}
