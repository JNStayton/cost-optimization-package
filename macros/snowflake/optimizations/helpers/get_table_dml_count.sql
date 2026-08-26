{% macro get_table_dml_count(database_name, schema_name, table_name, lookback_days=7) %}
  {#--
    Returns the count of DML operations (INSERT, UPDATE, DELETE, MERGE)
    against a specific table from QUERY_HISTORY.
    Standalone helper — queries Snowflake directly without model dependencies.

    Returns columns: DML_COUNT
  --#}

  {% set fqn = database_name ~ '.' ~ schema_name ~ '.' ~ table_name %}

  {% set sql %}
    select count(*) as dml_count
    from snowflake.account_usage.query_history
    where start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
      and query_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE')
      and contains(upper(replace(query_text, '"', '')), upper('{{ fqn }}'))
  {% endset %}

  {% set results = run_query(sql) %}
  {{ return(results) }}

{% endmacro %}
