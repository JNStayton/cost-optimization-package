{#--
  Post-hook that probes candidate unique keys for incremental-config recommendations.
  Dispatches to the platform implementation:
    - snowflake__probe_unique_key_candidates   (macros/snowflake/utils/snowflake__probe_unique_key_candidates.sql)
    - databricks__probe_unique_key_candidates  (macros/databricks/utils/databricks__probe_unique_key_candidates.sql)
    - redshift__probe_unique_key_candidates    (macros/redshift/utils/redshift__probe_unique_key_candidates.sql)
--#}
{% macro probe_unique_key_candidates() %}
  {{ return(adapter.dispatch('probe_unique_key_candidates', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__probe_unique_key_candidates() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "probe_unique_key_candidates is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: snowflake, databricks, redshift."
    ) }}
  {% endif %}
{% endmacro %}
