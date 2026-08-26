{% macro suggest_clustering_keys(model_name, include_boolean_cols=false) %}

  {#--
    Orchestrates all macros to suggest a clustering key for a given model.

    Use this in combination with the find_table_clustering_candidates macro. Once you have identified a table that may benefit from clustering, run this macro to get column-level recommendations on good clustering keys for that specific table.

    1. Calls get_clustering_cardinality_stats() to find structurally good candidates.
    2. For each candidate, calls get_column_usage_count() to find query history usage on filtering and joins.
    3. For each candidate, calls get_clustering_score() to get a weighted score based on the above criteria.
    4. Prints the top 3 recommendations.
    5. [TODO] Creates/updates a model with the information.

    Args:
      model_name: name of the dbt model to analyze (ref-able).
      include_boolean_cols (default false): when true, lowers the cardinality
        floor from > 10 distinct values to >= 2 so booleans and small enums are
        considered as candidates. Useful when query patterns are dominated by
        filters on small categorical columns or flag columns.

    How to run:
    dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name}'

    With low-cardinality columns included:
    dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name, include_boolean_cols: true}'
  --#}

  {% if execute %}

    {% set model_relation = ref(model_name) %}

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



{% macro get_clustering_cardinality_stats(model_relation, include_boolean_cols=false) %}
  {#--
    Queries the given relation to get cardinality statistics for each column.
    Filters out columns that are poor clustering candidates (e.g., unique keys
    or very low cardinality keys).

    When include_boolean_cols=true, the cardinality floor drops from > 10 to >= 2,
    allowing booleans and small enums through. Default behavior excludes those
    since they often produce too-coarse pruning on average workloads.

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
      {% if include_boolean_cols %}
      and cs.distinct_values >= 2 -- Include boolean and small-enum columns (include_boolean_cols=true)
      {% else %}
      and cs.distinct_values > 10 -- Exclude very low cardinality columns
      {% endif %}
    order by distinct_values desc
  {% endset %}

  {% set cardinality_results = run_query(cardinality_sql) %}

  {{ return(cardinality_results) }}

{% endmacro %}



{% macro get_column_usage_count(column_name, model_relation, days_to_check=7) %}
  {#--
    Queries SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to find how many times
    a column was used in a Filter or Join operator for a specific model,
    using GET_QUERY_OPERATOR_STATS for exact plan-level evidence.

    NOTE: The role running this macro must have privileges
    on SNOWFLAKE.ACCOUNT_USAGE.
  --#}

  {# Step 1: Find recent SELECT queries referencing this exact FQN #}
  {% set discovery_sql %}
    select query_id
    from snowflake.account_usage.query_history
    where start_time >= dateadd('day', -{{ days_to_check }}, current_timestamp())
      and query_type = 'SELECT'
      and contains(upper(replace(query_text, '"', '')), upper('{{ model_relation }}'))
      and query_text not ilike '%dbt_internal_test%'
      and query_text not ilike '%account_usage.query_history%'
      and query_text not ilike '%get_query_operator_stats%'
      and query_text not ilike '%approx_count_distinct%'
      and query_text not ilike '%int_snowflake__query_operator%'
      and (bytes_scanned > 0 or partitions_scanned > 0)
    order by start_time desc
    limit 20
  {% endset %}

  {% set discovery_results = run_query(discovery_sql) %}
  {% set query_ids = discovery_results.columns[0].values() if discovery_results and discovery_results.rows | length > 0 else [] %}

  {{ log("    [debug] discovered " ~ (query_ids | length) ~ " queries for " ~ column_name, info=true) }}

  {% set usage_count = 0 %}

  {% if query_ids | length > 0 %}
    {# Step 2: Check operator stats for this column in Filter/Join operators #}
    {% for qid in query_ids %}
      {% set col_pattern = '%' ~ column_name ~ '%' %}
      {% set op_sql %}
        select count(*) as hits
        from table(get_query_operator_stats('{{ qid }}'))
        where operator_type in ('Filter', 'Join')
          and (
            operator_attributes:filter_condition::string ilike '{{ col_pattern }}'
            or operator_attributes:equality_join_condition::string ilike '{{ col_pattern }}'
          )
      {% endset %}
      {% set op_result = run_query(op_sql) %}
      {% set hits = op_result.columns[0].values()[0] if op_result and op_result.rows | length > 0 else 0 %}
      {{ log("    [debug] query " ~ qid ~ " -> " ~ hits ~ " hits for " ~ column_name, info=true) }}
      {% if hits > 0 %}
        {% set usage_count = usage_count + 1 %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ log("    [debug] total usage_count for " ~ column_name ~ ": " ~ usage_count, info=true) }}
  {{ return(usage_count | string | int) }}

{% endmacro %}



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
      {# Calculate cardinality score as a percentage of total rows #}
      {% set cardinality_pct_score = (avg_rows / total_rows) * 100 %}

      {# Add weighted usage score. (Each use is worth 20 points) #}
      {% set recommendation_score = cardinality_pct_score + (usage_count * 20) %}
  {% endif %}

  {{ return(recommendation_score) }}

{% endmacro %}