{{
  config(
    materialized = 'ephemeral'
  )
}}

{#-- Routes to the platform-specific table inventory (tables + storage joined). --#}

{% if target.type == 'snowflake' %}

select * from {{ ref('int_snowflake__table_inventory') }}

{% elif target.type == 'bigquery' %}

select * from {{ ref('int_bigquery__table_inventory') }}

{% elif target.type == 'redshift' %}

select * from {{ ref('int_redshift__table_inventory') }}

{% elif target.type == 'databricks' %}

select * from {{ ref('int_databricks__table_inventory') }}

{% else %}

{{ exceptions.raise_compiler_error(
  "Unsupported adapter type: " ~ target.type ~
  ". Supported adapters are: snowflake, bigquery, databricks, redshift."
) }}

{% endif %}
