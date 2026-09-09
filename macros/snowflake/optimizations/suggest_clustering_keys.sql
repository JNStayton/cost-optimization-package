{% macro suggest_clustering_keys(model_name, database=none, schema=none, include_boolean_cols=false) %}

  {#--
    Orchestrates all macros to suggest a clustering key for a given model.

    Use this in combination with the find_table_clustering_candidates macro. Once you have
    identified a table that may benefit from clustering, run this macro to get column-level
    recommendations on good clustering keys for that specific table.

    1. Calls get_clustering_cardinality_stats() to find structurally good candidates.
    2. For each candidate, calls get_column_usage_count() to find query history usage.
    3. For each candidate, calls get_clustering_score() to get a weighted score.
    4. Prints the top 3 recommendations.

    Args:
      model_name: name of the dbt model (or table) to analyze.
      database (optional): override database. When provided with schema, bypasses ref()
        and targets the specified relation directly. Useful for analyzing production tables
        from a dev environment.
      schema (optional): override schema. Must be provided alongside database.
      include_boolean_cols (default false): when true, lowers the cardinality
        floor from > 10 distinct values to >= 2 so booleans and small enums are
        considered as candidates.

    How to run:
    dbt run-operation suggest_clustering_keys --args '{model_name: fct_order_items}'

    With explicit database/schema (analyze production from dev):
    dbt run-operation suggest_clustering_keys --args '{model_name: fct_order_items, database: MY_DB, schema: PROD_MARTS}'

    With low-cardinality columns included:
    dbt run-operation suggest_clustering_keys --args '{model_name: fct_order_items, include_boolean_cols: true}'
  --#}

  {% if execute %}

    {% if database and schema %}
      {% set model_relation = adapter.get_relation(database=database, schema=schema, identifier=model_name) %}
      {% if not model_relation %}
        {{ log("ERROR: Could not find relation " ~ database ~ "." ~ schema ~ "." ~ model_name ~ ". Check that the table exists and your role has access.", info=true) }}
        {{ return('') }}
      {% endif %}
    {% else %}
      {% set model_relation = ref(model_name) %}
    {% endif %}

    {{ log("--- Step 1: Analyzing column cardinality for '" ~ model_relation ~ "' ---", info=true) }}

    {% set cardinality_results = get_clustering_cardinality_stats(model_relation, include_boolean_cols=include_boolean_cols) %}

    {% if not cardinality_results or cardinality_results | length == 0 %}
      {{ log("Could not generate any cardinality suggestions. All columns may have very low (<10) or very high (unique) cardinality.", warning=true) }}
      {{ return('') }}
    {% endif %}

    {% set total_rows = cardinality_results[0]['TOTAL_ROWS'] | string | int %}
    {{ log(model_relation ~ " has a total row count of " ~ total_rows, info=true )}}
    {{ log("--- Step 2: Analyzing column usage from Snowflake's query history (last 7 days) ---", info=true) }}

    {% set column_recommendations = [] %}

    {% for cand_row in cardinality_results %}
      {% set column_name = cand_row['COLUMN_NAME'] %}
      {{ log("... analyzing usage for " ~ column_name, info=true) }}

      {# Call macro to get usage #}
      {% set usage_count = get_column_usage_count(column_name, model_relation, days_to_check=7) %}

      {% set avg_rows = cand_row['AVG_ROWS_PER_VALUE'] | string %}

      {# Call macro to get score #}
      {% set recommendation_score = get_clustering_score(avg_rows, total_rows, usage_count) %}

      {% do column_recommendations.append({
          'column_name': column_name,
          'distinct_values': cand_row['DISTINCT_VALUES'],
          'usage_count': usage_count,
          'score': recommendation_score
      }) %}
    {% endfor %}

    {% set sorted_recommendations = column_recommendations | sort(attribute='score', reverse=true) %}

    {{ log("\n--- Top 3 Clustering Key Candidates for " ~ model_relation ~ " ---", info=true) }}
    {{ log("Sorted by a score combining cardinality and actual query usage.", info=true) }}

    {% for rec in sorted_recommendations %}
      {% if loop.index <= 3 %}
        {{ log("  - Candidate " ~ loop.index ~ ": " ~ rec.column_name ~ " (Score: " ~ rec.score ~ ", Distinct: " ~ rec.distinct_values ~ ", Uses: " ~ rec.usage_count ~ ")", info=true) }}
      {% endif %}
    {% endfor %}

  {% endif %}

{% endmacro %}