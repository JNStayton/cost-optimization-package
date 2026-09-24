{{ config(
    materialized='incremental',
    unique_key=['query_id', 'child_query_sequence', 'segment_id', 'step_id'],
    on_schema_change='append_new_columns'
) }}

{# 1:1 incremental over stg_redshift__query_detail. 7-day lookback. #}

with source as (

    select * from {{ ref('stg_redshift__query_detail') }}
    {% if is_incremental() %}
    where start_time >= (select dateadd(day, -7, max(start_time)) from {{ this }})
    {% endif %}

)

select * from source
