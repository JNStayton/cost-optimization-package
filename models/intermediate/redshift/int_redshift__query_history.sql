{{ config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns'
) }}

{# 1:1 incremental over stg_redshift__query_history. Accumulates history
   beyond Redshift's SYS retention window. 7-day lookback absorbs late-arriving
   queries that completed after the previous incremental run.

   See docs/redshift/materialization-strategy.md for the broader policy. #}

with source as (

    select * from {{ ref('stg_redshift__query_history') }}
    {% if is_incremental() %}
    where start_time >= (select dateadd(day, -7, max(start_time)) from {{ this }})
    {% endif %}

)

select * from source
