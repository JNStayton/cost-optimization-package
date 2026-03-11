{% macro suggest_clustering_keys(model_name, preview_only=true) %}

  {#--
    Orchestrates all macros to suggest a clustering key for a given model.

    Use this in combination with the find_table_clustering_candidates macro. Once you have identified a table that may benefit from clustering, run this macro to get column-level recommendations on good clustering keys for that specific table.

    1. Calls get_clustering_cardinality_stats() to find structurally good candidates.
    2. For each candidate, calls get_column_usage_count() to find query history usage on filtering and joins.
    3. For each candidate, calls get_clustering_score() to get a weighted score based on the above criteria.
    4. Prints the top 3 recommendations.
    5. [TODO] Creates/updates a model with the information.

    How to run:
    dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name}'
  --#}

  {% if execute %}

    {% set model_relation = ref(model_name) %}

    {{ log("--- Step 1: Analyzing column cardinality for '" ~ model_relation ~ "' ---", info=true) }}

    {% set cardinality_results = get_clustering_cardinality_stats(model_relation) %}

    {% if not cardinality_results or cardinality_results | length == 0 %}
      {{ log("Could not generate any cardinality suggestions. All columns may have very low (<10) or very high (unique) cardinality.", warning=true) }}
      {{ return('') }}
    {% endif %}

    {% set total_rows = cardinality_results[0]['TOTAL_ROWS'] | int %}
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

    {% if preview_only %}

    {% for rec in sorted_recommendations %}
      {% if loop.index <= 3 %}
        {{ log("  - Candidate " ~ loop.index ~ ": " ~ rec.column_name ~ " (Score: " ~ (rec.score | round(2)) ~ ", Distinct: " ~ rec.distinct_values ~ ", Uses: " ~ rec.usage_count ~ ")", info=true) }}
      {% endif %}
    {% endfor %}

    {% else %}
    
    -- TODO
    {{ log("Populating model clustering_key_candidates with results...", info=true) }}

    {% endif %}

  {% endif %}

{% endmacro %}



{% macro get_clustering_cardinality_stats(model_relation) %}
  {#--
    Queries the given relation to get cardinality statistics for each column.
    Filters out columns that are poor clustering candidates (e.g., unique keys
    or very low cardinality keys).

    Returns an Agate table with:
    - column_name
    - distinct_values
    - total_rows
    - avg_rows_per_value
  --#}

  {% set cardinality_sql %}
    with column_stats as (
      {% for column in adapter.get_columns_in_relation(model_relation) %}
        select
          '{{ column.name | upper }}' as column_name,
          approx_count_distinct({{ adapter.quote(column.name) }}) as distinct_values
        from {{ model_relation }}
        {% if not loop.last %}union all{% endif %}
      {% endfor %}
    ),
    table_stats as (
      select count(*) as total_rows from {{ model_relation }}
    )
    select
      cs.column_name,
      cs.distinct_values,
      ts.total_rows,
      DIV0(ts.total_rows, cs.distinct_values) as avg_rows_per_value
    from column_stats cs
    cross join table_stats ts
    where cs.distinct_values < ts.total_rows -- Exclude unique keys
      and cs.distinct_values > 10 -- Exclude very low cardinality columns
    order by distinct_values desc
  {% endset %}

  {% set cardinality_results = run_query(cardinality_sql) %}

  {{ return(cardinality_results) }}

{% endmacro %}



{% macro get_column_usage_count(column_name, model_relation, days_to_check=7) %}
  {#--
    Queries SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to find how many times
    a column was used in a JOIN or WHERE clause for a specific model.

    NOTE: The role running this macro must have privileges
    on SNOWFLAKE.ACCOUNT_USAGE.
  --#}

  {% set usage_sql %}
    select
      count(*) as usage_count
    from snowflake.account_usage.query_history
    where start_time >= dateadd('day', -{{ days_to_check }}, current_timestamp())
      and (
        query_text ilike '%JOIN%ON%{{ column_name }}%'
        or query_text ilike '%WHERE%{{ column_name }}%'
      )
      and query_text ilike '%{{ model_relation.identifier }}%'
  {% endset %}

  {% set usage_results = run_query(usage_sql) %}

  {% set usage_count = usage_results.columns[0].values()[0] if usage_results else 0 %}

  {{ return(usage_count | int) }}

{% endmacro %}



{% macro get_clustering_score(avg_rows, total_rows, usage_count) %}
  {#--
    Calculates a recommendation score based on cardinality and usage.
    Gives a heavy weighting to columns that are actually used in queries.
  --#}
  {% set recommendation_score = 0 %}
  {% set avg_rows = avg_rows | float %}
  {% set total_rows = total_rows | float %}
  {% set usage_count = usage_count | int %}

  {% if total_rows > 0 %}
      {# Calculate cardinality score as a percentage of total rows #}
      {% set cardinality_pct_score = (avg_rows / total_rows) * 100 %}

      {# Add weighted usage score. (Each use is worth 20 points) #}
      {% set recommendation_score = cardinality_pct_score + (usage_count * 20) %}
  {% endif %}

  {{ return(recommendation_score) }}

{% endmacro %}