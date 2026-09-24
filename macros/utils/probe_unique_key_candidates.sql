{#--
  Post-hook that probes candidate unique keys for incremental-config recommendations.
  Dispatches to the platform implementation:
    - snowflake__probe_unique_key_candidates   (macros/snowflake/utils/)
    - databricks__probe_unique_key_candidates  (macros/databricks/utils/)
    - redshift__probe_unique_key_candidates    (macros/redshift/utils/)
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
