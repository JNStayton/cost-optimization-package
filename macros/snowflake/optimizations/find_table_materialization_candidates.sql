{% macro find_table_materialization_candidates(lookback_days=14, min_query_count=10) %}

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

    {% set is_enterprise = var('snowflake_enterprise_edition', true) %}

    {{ log("--- Starting Materialization Candidate Analysis ---", info=true) }}
    {{ log("Criteria: View queried > " ~ min_query_count ~ " times in last " ~ lookback_days ~ " days.", info=true) }}

    {# --- Build list of view models from the dbt graph --- #}
    {% set view_models = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
        {% if node.config.materialized == 'view' and node.database and node.schema %}
            {% set node_identifier = node.alias if node.alias else node.name %}
            {% do view_models.append({
                'database': node.database | upper,
                'schema': node.schema | upper,
                'table': node_identifier | upper,
                'fqn': (node.database | upper) ~ '.' ~ (node.schema | upper) ~ '.' ~ (node_identifier | upper),
                'node': node
            }) %}
        {% endif %}
    {% endfor %}

    {% if is_enterprise %}

    {# --- Enterprise: use ACCESS_HISTORY for exact object-level attribution --- #}
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
          materialization_score,
          case
              when materialization_score > 500 and total_gb_scanned > 10 then 'large_scan'
              when avg_elapsed_seconds > 10 and total_queries_last_{{ lookback_days }}_days > 50 then 'slow_frequent'
              else 'monitor'
          end as recommendation_key
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
        
        {# namespace() avoids Jinja loop-scoping bug #}
        {% set ns = namespace(model_node=none) %}
        {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            {% set node_identifier = node.alias if node.alias else node.name %}
            {% if node.database
                  and node.schema
                  and node_identifier
                  and node.database | upper == db | upper
                  and node.schema | upper == sc | upper
                  and node_identifier | upper == tb | upper %}
                {% set ns.model_node = node %}
                {% break %}
            {% endif %}
        {% endfor %}

        {% set current_materialization = ns.model_node.config.materialized if ns.model_node else 'N/A (Not in dbt project)' %}
        {% set recommendation = 'Monitor' %}
        {% set recommendation_reason = 'Low Priority' %}

        {% set recommendation_key = row['RECOMMENDATION_KEY'] %}
        {% if current_materialization != 'N/A (Not in dbt project)' %}
            {% if recommendation_key == 'large_scan' %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Large Scan' %}
            {% elif recommendation_key == 'slow_frequent' %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Slow performance, frequently queried' %}
            {% endif %}
        {% endif %}

        {% do candidates.append({
            'fqn': fqn_key,
            'dbt_materialization': current_materialization,
            'total_queries': row["TOTAL_QUERIES_LAST_" ~ lookback_days ~ "_DAYS"],
            'avg_elapsed_sec': row["AVG_ELAPSED_SECONDS"],
            'total_gb_scanned': row["TOTAL_GB_SCANNED"],
            'materialization_score': row["MATERIALIZATION_SCORE"],
            'recommendation': recommendation,
            'recommendation_reason': recommendation_reason
        }) %}
        
    {% endfor %}

    {% else %}

    {# --- Standard: use query_text matching per view model --- #}
    {% set candidates = [] %}

    {{ log("--- Step 2: Checking query volume for " ~ (view_models | length) ~ " view models (Standard edition) ---", info=true) }}

    {% for vm in view_models %}
        {% set view_perf_sql %}
            select
                count(*) as total_queries,
                avg(total_elapsed_time) / 1000.0 as avg_elapsed_seconds,
                sum(bytes_scanned) / power(1024, 3) as total_gb_scanned
            from snowflake.account_usage.query_history
            where start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and execution_status = 'SUCCESS'
              and query_type = 'SELECT'
              and contains(upper(replace(query_text, '"', '')), '{{ vm.fqn }}')
        {% endset %}

        {% set perf_result = run_query(view_perf_sql) %}
        {% set total_queries = perf_result.columns[0].values()[0] | int if perf_result and perf_result.rows | length > 0 else 0 %}

        {% if total_queries > min_query_count %}
            {% set avg_elapsed = perf_result.columns[1].values()[0] | float if perf_result else 0 %}
            {% set total_gb = perf_result.columns[2].values()[0] | float if perf_result else 0 %}
            {% set mat_score = total_queries * avg_elapsed %}

            {% set recommendation = 'Monitor' %}
            {% set recommendation_reason = 'Low Priority' %}
            {% if mat_score > 500 and total_gb > 10 %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Large Scan' %}
            {% elif avg_elapsed > 10 and total_queries > 50 %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Slow performance, frequently queried' %}
            {% elif total_queries > min_query_count %}
                {% set recommendation = 'Materialize as TABLE' %}
                {% set recommendation_reason = 'Frequently queried view' %}
            {% endif %}

            {% do candidates.append({
                'fqn': vm.fqn,
                'dbt_materialization': 'view',
                'total_queries': total_queries,
                'avg_elapsed_sec': avg_elapsed,
                'total_gb_scanned': total_gb,
                'materialization_score': mat_score,
                'recommendation': recommendation,
                'recommendation_reason': recommendation_reason
            }) %}
        {% endif %}
    {% endfor %}

    {% set candidates = candidates | sort(attribute='materialization_score', reverse=true) %}

    {% endif %}

    {# --- results --- #}
    {# SQL already orders by materialization_score desc — preserve that order #}
    {% set sorted_candidates = candidates %}

    {{ log("\n--- Top VIEWS that should be materialized as TABLEs ---", info=true) }}
    {{ log("-----------------------------------------------------------------------", info=true) }}

    {% if sorted_candidates | length == 0 %}
        {{ log("No materialization candidates found.", info=true) }}
        {{ log("", info=true) }}
        {{ log("This means no views in the current project exceeded " ~ min_query_count ~ " queries", info=true) }}
        {{ log("in the last " ~ lookback_days ~ " days.", info=true) }}
        {{ log("", info=true) }}
        {{ log("Suggestions:", info=true) }}
        {{ log("  - Try extending lookback_days (e.g., --args '{lookback_days: 30}')", info=true) }}
        {{ log("  - Try lowering min_query_count (e.g., --args '{min_query_count: 5}')", info=true) }}
        {{ log("  - Verify IMPORTED PRIVILEGES is granted on the SNOWFLAKE database", info=true) }}
    {% endif %}

    {% for c in sorted_candidates %}
        {% if loop.index > 20 %}{% break %}{% endif %}

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