{#-- Routes to the platform-specific tables metadata intermediate model --#}

{% if target.type == 'snowflake' %}

select * from {{ ref('int_snowflake__tables') }}

{% elif target.type == 'bigquery' %}

select * from {{ ref('int_bigquery__tables') }}

{% elif target.type == 'redshift' %}

select * from {{ ref('int_redshift__tables') }}

{% elif target.type == 'databricks' %}

select * from {{ ref('int_databricks__tables') }}

{% else %}

{{ exceptions.raise_compiler_error(
  "Unsupported adapter type: " ~ target.type ~
  ". Supported adapters are: snowflake, bigquery, databricks, redshift."
) }}

{% endif %}