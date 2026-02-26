{{
  config(
    materialized = 'ephemeral'
  )
}}

{#-- Routes to the platform-specific query history intermediate model --#}

{% if target.type == 'snowflake' %}

select * from {{ ref('int_snowflake__query_history') }}

{% elif target.type == 'bigquery' %}

select * from {{ ref('int_bigquery__query_history') }}

{% elif target.type == 'redshift' %}

select * from {{ ref('int_redshift__query_history') }}

{% elif target.type == 'databricks' %}

select * from {{ ref('int_databricks__query_history') }}

{% else %}

{{ exceptions.raise_compiler_error(
  "Unsupported adapter type: " ~ target.type ~
  ". Supported adapters are: snowflake, bigquery, databricks, redshift."
) }}

{% endif %}