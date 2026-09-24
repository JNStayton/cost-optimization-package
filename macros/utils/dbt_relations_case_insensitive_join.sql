{#--
  Dispatcher for dbt_relations_case_insensitive_join. Platform implementations:
    - bigquery__dbt_relations_case_insensitive_join   (macros/bigquery/utils/dbt_relations_case_insensitive_join.sql)
  Add a <platform>__dbt_relations_case_insensitive_join with the same arguments to support another platform.
--#}
{% macro dbt_relations_case_insensitive_join(left_alias) %}
  {{ return(adapter.dispatch('dbt_relations_case_insensitive_join', 'dbt_cost_optimization_package')(
      left_alias=left_alias
  )) }}
{% endmacro %}

{% macro default__dbt_relations_case_insensitive_join(left_alias) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error(
        "dbt_relations_case_insensitive_join is not yet implemented for '" ~ target.type ~ "'. "
        ~ "Supported platforms: bigquery."
    ) }}
  {% endif %}
{% endmacro %}
