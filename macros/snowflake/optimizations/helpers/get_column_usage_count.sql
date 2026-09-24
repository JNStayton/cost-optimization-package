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
      and contains(upper(replace(query_text, '"', '')), upper(replace('{{ model_relation }}', '"', '')))
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

  {% set ns = namespace(usage_count=0) %}

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
      {% if hits > 0 %}
        {% set ns.usage_count = ns.usage_count + 1 %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ return(ns.usage_count) }}

{% endmacro %}
