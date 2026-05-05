{% macro find_incremental_materialization_candidates(min_table_size_gb=10, max_build_time_sec=600, lookback_days=30, preview_only=true) %}

  {#--
    Identifies dbt models currently configured as TABLEs that are large, slow to build, 
    and contain suitable columns for conversion to incremental materialization.

    1. Finds large, slow-building tables via Snowflake ACCOUNT_USAGE.
    2. Joins with the dbt graph to ensure model is currently 'table'.
    3. Checks the table structure for suitable date/timestamp keys for the microbatch strategy.
    4. [TODO] Creates/updates a model with the information.

    How to run:
    dbt run-operation find_incremental_candidates

    How to run with custom args:

    dbt run-operation find_incremental_candidates --args '{min_table_size_gb: 100, max_build_time_sec: 6000, lookback_days: 7}'
  --#}

  {% if execute %}
    
    {{ log("--- Starting Incremental Candidate Analysis ---", info=true) }}
    {{ log("Criteria: Table > " ~ min_table_size_gb ~ " GB & Build Time > " ~ max_build_time_sec ~ "s in last " ~ lookback_days ~ " days.", info=true) }}

    {# --- snowflake performance and size query --- #}
    {% set incremental_sql %}
      with model_performance as (
          select
              qh.query_id,
              qh.total_elapsed_time,
              qh.start_time,
              qh.query_text,
              t.table_catalog as database_name,
              t.table_schema as schema_name,
              t.table_name as table_name
          from
              snowflake.account_usage.query_history qh
          inner join
              snowflake.account_usage.access_history h on qh.query_id = h.query_id
          , lateral flatten(input => h.objects_modified) f
          inner join
              snowflake.account_usage.tables t on f.value:objectId::number = t.table_id
          where
              qh.total_elapsed_time / 1000 > {{ max_build_time_sec }}
              and qh.start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and qh.execution_status = 'SUCCESS'
              and (qh.query_text ilike 'CREATE TABLE%AS%SELECT%' or qh.query_text ilike 'CREATE OR REPLACE TABLE%')
      ),
      
      table_size as (
          select
              table_catalog as database_name,
              table_schema as schema_name,
              table_name,
              round(active_bytes / power(1024, 3), 2) as size_gb
          from
              snowflake.account_usage.table_storage_metrics
          where
              active_bytes / power(1024, 3) >= {{ min_table_size_gb }}
      )
      
      select
          mp.database_name,
          mp.schema_name,
          mp.table_name,
          ts.size_gb,
          count(mp.query_id) as total_slow_runs,
          max(mp.total_elapsed_time / 1000) as max_build_time_sec,
          avg(mp.total_elapsed_time / 1000) as avg_build_time_sec
      from
          model_performance mp
      inner join
          table_size ts on 
              mp.database_name = ts.database_name and mp.schema_name = ts.schema_name and mp.table_name = ts.table_name
      group by 1, 2, 3, 4
      order by max_build_time_sec desc
      limit 100
    {% endset %}

    {% set performance_results = run_query(incremental_sql) %}

    {% set candidates = [] %}

    {{ log("--- Joining with dbt graph and checking table structure ---", info=true) }}

    {% for row in performance_results.rows %}
        {% set db = row["DATABASE_NAME"] %}
        {% set sc = row["SCHEMA_NAME"] %}
        {% set tb = row["TABLE_NAME"] %}
        {% set fqn_key = db ~ "." ~ sc ~ "." ~ tb %}
        
        {# check dbt materialization #}
        {% set model_node = none %}
        {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            {% set node_fqn = node.database ~ "." ~ node.schema ~ "." ~ node.alias | default(node.name) %}
            {% if node_fqn | upper == fqn_key | upper %}
                {% set model_node = node %}
                {% break %}
            {% endif %}
        {% endfor %}

        {% set current_materialization = model_node.config.materialized if model_node else 'N/A' %}
        
        {% set is_incremental_candidate = false %}
        {% set incremental_key_suggestion = 'N/A' %}

        {% if current_materialization == 'table' %}
            {# check for common incremental keys: date, timestamp, id #}
            {% set column_check_sql %}
                select 
                    listagg(column_name, ', ') 
                from information_schema.columns 
                where table_catalog = '{{ db }}' 
                  and table_schema = '{{ sc }}' 
                  and table_name = '{{ tb }}'
                  and data_type in ('DATE', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'NUMBER')
                  and column_name ilike any ('%_at', '%_date', 'date%', '%dt', '%_id')
            {% endset %}

            {% set key_results = run_query(column_check_sql) %}
            {% set suitable_keys = key_results.columns[0].values()[0] if key_results.columns[0].values() else none %}

            {% if suitable_keys %}
                {% set is_incremental_candidate = true %}
                {% set incremental_key_suggestion = suitable_keys %}
            {% endif %}
        {% endif %}

        {% set recommendation = 'Monitor' %}
        {% if is_incremental_candidate %}
            {% set recommendation = 'Materialize as INCREMENTAL' %}
        {% elif current_materialization == 'table' %}
            {% set recommendation = 'Verify keys (Manual Check)' %}
        {% endif %}


        {% do candidates.append({
            'fqn': fqn_key,
            'dbt_materialization': current_materialization,
            'size_gb': row["SIZE_GB"],
            'max_build_time_sec': (row["MAX_BUILD_TIME_SEC"] | round(0)),
            'avg_build_time_sec': (row["AVG_BUILD_TIME_SEC"] | round(0)),
            'total_slow_runs': row["TOTAL_SLOW_RUNS"],
            'suitable_keys': incremental_key_suggestion,
            'recommendation': recommendation
        }) %}
        
    {% endfor %}

    {# --- results --- #}
    {% set sorted_candidates = candidates | sort(attribute="max_build_time_sec", reverse=true) %}

    {{ log("\n--- Top TABLEs that should be materialized as INCREMENTAL ---", info=true) }}
    {{ log("-----------------------------------------------------------------------", info=true) }}

    {% for c in sorted_candidates %}
        {% if preview_only and loop.index > 10 %}{% break %}{% endif %}

        {% if c.dbt_materialization == 'table' or not preview_only %}
            {{ log("Model: " ~ c.fqn, info=true) }}
            {{ log("  - Current dbt Materialization: " ~ c.dbt_materialization, info=true) }}
            {{ log("  - Table Size: " ~ c.size_gb ~ " GB", info=true) }}
            {{ log("  - Max Build Time: " ~ c.max_build_time_sec ~ "s (Slow Runs: " ~ c.total_slow_runs ~ ")", info=true) }}
            {{ log("  - Recommendation: " ~ c.recommendation, info=true) }}
            {% if c.recommendation == 'Change to INCREMENTAL' %}
                {{ log("  - Suggested Key(s) to Test: " ~ c.suitable_keys, info=true) }}
            {% endif %}
            {{ log("---", info=true) }}
        {% endif %}
    {% endfor %}

  {% endif %}

{% endmacro %}