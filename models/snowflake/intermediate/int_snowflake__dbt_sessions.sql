{{
  config(
    materialized='incremental',
    unique_key='session_id',
    cluster_by=['to_date(created_on)'],
    on_schema_change='append_new_columns',
  )
}}

{#--
  Persists dbt-authenticated sessions from ACCOUNT_USAGE.SESSIONS.
  Replaces the inline dbt_session_filter CTE that is duplicated across every
  macro and dashboard query. Downstream warehouse intermediates join to this
  model instead of re-deriving session attribution on each run.

  Grain: one row per session_id where APPLICATION = 'dbt'.

  Incremental strategy: merge on session_id. The 1-day overlap on incremental
  loads handles the up-to-3-hour ACCOUNT_USAGE latency without re-scanning the
  full history on every run.
--#}

select
    session_id,
    created_on,
    application_name,
    client_os,
    client_version
from {{ ref('stg_snowflake__sessions') }}
{% if is_incremental() %}
where created_on >= (select dateadd(day, -{{ var('incremental_overlap_days', 31) }}, max(created_on)) from {{ this }})
{% endif %}
