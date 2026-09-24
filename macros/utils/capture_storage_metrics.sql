{#--
  Dispatcher for capture_storage_metrics. Platform implementations:
    - databricks__capture_storage_metrics   (macros/platforms/databricks/utils/databricks__capture_storage_metrics.sql)
  Add a <platform>__capture_storage_metrics with the same arguments to support another platform.
--#}
{% macro capture_storage_metrics() %}
  {{ return(adapter.dispatch('capture_storage_metrics', 'dbt_cost_optimization_package')()) }}
{% endmacro %}

{% macro default__capture_storage_metrics() %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "capture_storage_metrics is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: databricks."
    ) }}
  {% endif %}
{% endmacro %}
