{% macro find_incremental_materialization_candidates(min_table_size_gb=10, max_build_time_sec=600, lookback_days=30, preview_only=true) %}

  {#--
    Identifies dbt models currently configured as TABLEs that are large, slow to build,
    and contain suitable columns for conversion to incremental materialization.

    1. Finds large, slow-building tables via Snowflake ACCOUNT_USAGE.
    2. Joins with the dbt graph to ensure model is currently 'table'.
    3. Checks the table structure for suitable date/timestamp keys for the microbatch strategy.
    4. Assigns a priority tier based on size and avg build time.
    5. [TODO] Creates/updates a model with the information.

    Priority tiers:
      HIGH:   size > 100 GB AND avg build > 30 min
      MEDIUM: size > 50 GB OR avg build > 15 min (and not HIGH)
      LOW:    above the qualifying floor but below MEDIUM

    For full incremental strategy + key recommendations and template code, run the
    materialization-analysis DAG bundled with this package.

    How to run:
    dbt run-operation find_incremental_materialization_candidates

    How to run with custom args:

    dbt run-operation find_incremental_materialization_candidates --args '{min_table_size_gb: 100, max_build_time_sec: 6000, lookback_days: 7}'
  --#}

  {% if execute %}
    
    {{ log("--- Starting Incremental Candidate Analysis ---", info=true) }}
    {{ log("Criteria: Table > " ~ min_table_size_gb ~ " GB & Build Time > " ~ max_build_time_sec ~ "s in last " ~ lookback_days ~ " days.", info=true) }}
    {{ log("Priority tiers: HIGH (size > 100 GB AND avg build > 30 min), MEDIUM (size > 50 GB OR avg build > 15 min), LOW (above floor but below MEDIUM).", info=true) }}

    {# --- snowflake performance and size query --- #}
    {% set incremental_sql %}
      with model_performance as (
          select
              qh.query_id,
              qh.total_elapsed_time,
              qh.start_time,
              qh.query_text,
              qh.query_type,
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
              and (
                  qh.query_type in ('CREATE_TABLE', 'CREATE_TABLE_AS_SELECT')
                  or qh.query_text ilike 'create%table%as%select%'
                  or qh.query_text ilike 'create or replace%table%'
              )
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
          avg(mp.total_elapsed_time / 1000) as avg_build_time_sec,
          min(mp.query_type) as sample_query_type,
          case
              when ts.size_gb > 100 and avg(mp.total_elapsed_time / 1000) > 1800 then 'HIGH'
              when ts.size_gb > 50 or avg(mp.total_elapsed_time / 1000) > 900 then 'MEDIUM'
              else 'LOW'
          end as priority_key,
          case
              when ts.size_gb > 100 and avg(mp.total_elapsed_time / 1000) > 1800 then 3
              when ts.size_gb > 50 or avg(mp.total_elapsed_time / 1000) > 900 then 2
              else 1
          end as priority_rank
      from
          model_performance mp
      inner join
          table_size ts
          on mp.database_name = ts.database_name
         and mp.schema_name = ts.schema_name
         and mp.table_name = ts.table_name
      group by 1, 2, 3, 4
      order by priority_rank desc, max_build_time_sec desc
      limit 100
    {% endset %}

    {% set performance_results = run_query(incremental_sql) %}

    {% set candidates = [] %}

    {{ log("--- Joining with dbt graph and checking table structure ---", info=true) }}
    {{ log("Initial warehouse candidates: " ~ (performance_results.rows | length), info=true) }}

    {% for row in performance_results.rows %}
        {% set db = row["DATABASE_NAME"] %}
        {% set sc = row["SCHEMA_NAME"] %}
        {% set tb = row["TABLE_NAME"] %}
        {% set fqn_key = db ~ "." ~ sc ~ "." ~ tb %}
        
        {# check dbt materialization #}
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

        {% set current_materialization = ns.model_node.config.materialized if ns.model_node else 'N/A' %}
        {{ log("DEBUG candidate: " ~ fqn_key ~ " | query_type=" ~ row["SAMPLE_QUERY_TYPE"] ~ " | materialization=" ~ current_materialization, info=true) }}
        
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
            'max_build_time_sec': row["MAX_BUILD_TIME_SEC"],
            'avg_build_time_sec': row["AVG_BUILD_TIME_SEC"],
            'total_slow_runs': row["TOTAL_SLOW_RUNS"],
            'sample_query_type': row["SAMPLE_QUERY_TYPE"],
            'suitable_keys': incremental_key_suggestion,
            'priority': row["PRIORITY_KEY"],
            'recommendation': recommendation
        }) %}
        
    {% endfor %}

    {# --- results — SQL already ordered by priority_rank desc, max_build_time_sec desc --- #}
    {% set sorted_candidates = candidates %}

    {{ log("\n--- Top TABLEs that should be materialized as INCREMENTAL ---", info=true) }}
    {{ log("-----------------------------------------------------------------------", info=true) }}

    {% set table_candidates = sorted_candidates | selectattr('dbt_materialization', 'equalto', 'table') | list %}

    {{ log("Candidates after dbt graph join: " ~ (candidates | length), info=true) }}
    {{ log("dbt table matches: " ~ (table_candidates | length), info=true) }}

    {% if table_candidates | length == 0 %}
        {{ log("No active recommendations for this project.", info=true) }}
        {{ log("", info=true) }}
        {% if performance_results.rows | length == 0 %}
            {{ log("No Snowflake table builds met the warehouse criteria.", info=true) }}
            {{ log("This means no successful table-build statements exceeded the", info=true) }}
            {{ log("build-time threshold (" ~ max_build_time_sec ~ "s) on tables >= " ~ min_table_size_gb ~ " GB", info=true) }}
            {{ log("in the last " ~ lookback_days ~ " days.", info=true) }}
        {% else %}
            {{ log("Snowflake candidates were found, but none mapped to current dbt TABLE models in this project.", info=true) }}
            {{ log("This usually means either:", info=true) }}
            {{ log("  - the matched relations are not current dbt models in this target, or", info=true) }}
            {{ log("  - the dbt graph relation/database/schema/identifier mapping did not line up.", info=true) }}
        {% endif %}
        {{ log("", info=true) }}
        {{ log("Suggestions:", info=true) }}
        {{ log("  - Try extending lookback_days (e.g., --args '{lookback_days: 60}')", info=true) }}
        {{ log("  - Try lowering min_table_size_gb (e.g., --args '{min_table_size_gb: 1}')", info=true) }}
        {{ log("  - Try lowering max_build_time_sec (e.g., --args '{max_build_time_sec: 10}')", info=true) }}
    {% else %}

    {% for c in table_candidates %}
        {% if preview_only and loop.index > 10 %}{% break %}{% endif %}

        {{ log("[" ~ c.priority ~ "] Model: " ~ c.fqn, info=true) }}
        {{ log("  - Current dbt Materialization: " ~ c.dbt_materialization, info=true) }}
        {{ log("  - Sample Query Type: " ~ c.sample_query_type, info=true) }}
        {{ log("  - Table Size: " ~ c.size_gb ~ " GB", info=true) }}
        {{ log("  - Avg Build Time: " ~ c.avg_build_time_sec ~ "s (Max: " ~ c.max_build_time_sec ~ "s, Slow Runs: " ~ c.total_slow_runs ~ ")", info=true) }}
        {{ log("  - Recommendation: " ~ c.recommendation, info=true) }}
        {% if c.recommendation == 'Materialize as INCREMENTAL' %}
            {{ log("  - Suggested Key(s) to Test: " ~ c.suitable_keys, info=true) }}
            {{ log("  - Tip: run the materialization-analysis DAG for full incremental strategy + key recommendations and template code.", info=true) }}
        {% endif %}
        {{ log("---", info=true) }}
    {% endfor %}

    {% endif %}

  {% endif %}

{% endmacro %}