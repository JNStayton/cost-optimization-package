{% macro find_table_materialization_candidates(lookback_days=14, min_query_count=10, preview_only=true) %}

  {#--
    Identifies dbt models currently configured as VIEWs that are experiencing high 
    query volume, long execution times, or large data scans.
    
    This macro executes a Snowflake query to get performance data and then 
    joins it with the dbt graph object to verify current materialization.

    [TODO] Create/update a model with the information.

    How to run:
    dbt run-operation find_table_materialization_candidates

    How to run with custom args:

    dbt run-operation find_table_materialization_candidates --args '{lookback_days: 7, min_query_count: 30}'
  --#}

  {% if execute %}

    {{ log("--- Starting Materialization Candidate Analysis ---", info=true) }}
    {{ log("Criteria: View queried > " ~ min_query_count ~ " times in last " ~ lookback_days ~ " days.", info=true) }}

    {# --- snowflake performance query --- #}
    {% set performance_sql %}
      with frequent_view_queries as (
          select
              q.query_id,
              q.total_elapsed_time,
              q.bytes_scanned,
              t.table_catalog as database_name,
              t.table_schema as schema_name,
              t.table_name as view_name
          from
              snowflake.account_usage.query_history q
          inner join
              snowflake.account_usage.access_history h on q.query_id = h.query_id
          , lateral flatten(input => h.base_objects_accessed) f 
          inner join
              snowflake.account_usage.tables t on f.value:objectId::number = t.table_id 
          where
              q.start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and q.execution_status = 'SUCCESS'
              and t.table_type = 'VIEW'
      ),

      view_performance_summary as (
          select
              view_name,
              database_name,
              schema_name,
              count(query_id) as total_queries_last_{{ lookback_days }}_days,
              avg(total_elapsed_time / 1000) as avg_elapsed_seconds,
              sum(bytes_scanned / power(1024, 3)) as total_gb_scanned,
              (count(query_id) * avg(total_elapsed_time / 1000)) as materialization_score
          from
              frequent_view_queries
          group by
              1, 2, 3
      )

      select
          view_name,
          database_name,
          schema_name,
          total_queries_last_{{ lookback_days }}_days,
          avg_elapsed_seconds,
          total_gb_scanned,
          materialization_score
      from
          view_performance_summary
      where
          total_queries_last_{{ lookback_days }}_days > {{ min_query_count }}
      order by
          materialization_score desc
      limit 100
    {% endset %}

    {% set performance_results = run_query(performance_sql) %}

    {% set candidates = [] %}

    {{ log("--- Step 2: Joining performance data with dbt graph ---", info=true) }}

    {# --- join with dbt graph --- #}
    {% for row in performance_results.rows %}
        {% set db = row["DATABASE_NAME"] %}
        {% set sc = row["SCHEMA_NAME"] %}
        {% set tb = row["VIEW_NAME"] %}
        {% set fqn_key = db ~ "." ~ sc ~ "." ~ tb %}
        
        {% set model_node = none %}
        {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            {% set node_fqn = node.database ~ "." ~ node.schema ~ "." ~ node.alias | default(node.name) %}
            {% if node_fqn | upper == fqn_key | upper %}
                {% set model_node = node %}
                {% break %}
            {% endif %}
        {% endfor %}

        {% set current_materialization = model_node.config.materialized if model_node else 'N/A (Not in dbt project)' %}
        {% set recommendation = 'Monitor' %}
        {% set recommendation_reason = 'Low Priority'%}
        
        {# scoring logic #}
        {% if current_materialization != 'N/A (Not in dbt project)' %}
            {% if row['MATERIALIZATION_SCORE'] > 500 and row['TOTAL_GB_SCANNED'] > 10 %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Large Scan' %}
            {% elif row['AVG_ELAPSED_SECONDS'] > 10 and row['TOTAL_QUERIES_LAST_' ~ lookback_days ~ '_DAYS'] > 50 %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Slow performance, frequently queried' %}
            {% endif %}
        {% endif %}

        {% do candidates.append({
            'fqn': fqn_key,
            'dbt_materialization': current_materialization,
            'total_queries': row["TOTAL_QUERIES_LAST_" ~ lookback_days ~ "_DAYS"],
            'avg_elapsed_sec': (row["AVG_ELAPSED_SECONDS"] | round(2)),
            'total_gb_scanned': (row["TOTAL_GB_SCANNED"] | round(2)),
            'materialization_score': (row["MATERIALIZATION_SCORE"] | round(0)),
            'recommendation': recommendation,
            'recommendation_reason': recommendation_reason
        }) %}
        
    {% endfor %}

    {# --- results --- #}
    {% set sorted_candidates = candidates | sort(attribute="materialization_score", reverse=true) %}

    {{ log("\n--- Top VIEWS that should be materialized as TABLEs ---", info=true) }}
    {{ log("-----------------------------------------------------------------------", info=true) }}

    {% for c in sorted_candidates %}
        {% if preview_only and loop.index > 10 %}{% break %}{% endif %}

        {{ log("Model: " ~ c.fqn, info=true) }}
        {{ log("  - Current dbt Materialization: " ~ c.dbt_materialization, info=true) }}
        {{ log("  - Recommendation: " ~ c.recommendation, info=true) }}
        {{ log("  - Reason for Recommendation: " ~ c.recommendation_reason, info=true) }}
        {{ log("  - Query Metrics -", info=true) }}
        {{ log("Total Queries:" ~ c.total_queries ~ " | Avg Time: " ~ c.avg_elapsed_sec ~ "s | Total Scanned: " ~ c.total_gb_scanned ~ " GB", info=true) }}
        {{ log("---", info=true) }}
    {% endfor %}

  {% endif %}

{% endmacro %}