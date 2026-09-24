{% macro get_clustering_score(avg_rows, total_rows, usage_count) %}
  {#--
    Calculates a recommendation score based on cardinality and usage.
    Gives a heavy weighting to columns that are actually used in queries.
  --#}
  {% set recommendation_score = 0 %}
  {% set avg_rows = avg_rows | string | float %}
  {% set total_rows = total_rows | string | float %}
  {% set usage_count = usage_count | string | int %}

  {% if total_rows > 0 %}
      {% set cardinality_pct_score = (avg_rows / total_rows) * 100 %}
      {% set recommendation_score = cardinality_pct_score + (usage_count * 20) %}
  {% endif %}

  {{ return(recommendation_score) }}

{% endmacro %}
