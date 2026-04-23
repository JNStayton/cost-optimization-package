{% macro find_table_clustering_candidates(lookback_days=7, ignore_table_size=false, dbt_project_only=true, target_databases=[], target_schemas=[], preview_only=true) %}

  {#--
    Orchestrates all macros to suggest tables in project that might benefit from clustering.

    This will log the top 10 candidates for clustering. Start here, implement the fixes where applicable, and then run again.

    You can optionally pass in a list of target databases/schemas to scan, or set to scan the entire account.
    
    By default, we are only evaluating tables that are at least 1TB in size, but you can set ignore_table_size to true to evaluate all tables regardless of size (useful for dev environments or if you want to find smaller candidates).

    [TODO] Creates/updates a model with the information for historical analysis. preview_only flag will just print the results in the logs without creating/updating the model. Right now the logic to create/update a model is not present, so this flag doesn't do anything... yet!

    How to run:
    dbt run-operation find_table_clustering_candidates 

    How to run with custom args:

    dbt run-operation find_table_clustering_candidates --args '{lookback_days: 10, target_databases: ['db_1', 'db_2']}'
  --#}

    {% if ignore_table_size %} 
        {% set min_size_gb = 1 %}
    {% else %} 
        {% set min_size_gb = 100 %}
    {% endif %}

    {% if execute %}

        {{ log("--- Starting Clustering Candidate Analysis ---", info=true) }}
        {{ log("Criteria: Table > " ~ min_size_gb ~ " GB | Lookback: " ~ lookback_days ~ " days | Must be dbt model: " ~ dbt_project_only, info=true) }}

        {# --- 1. Build dbt Model List --- #}
        {% set model_list = {} %}
        {% for node in graph.nodes.values() | selectattr(
            "resource_type", "equalto", "model"
        ) %}
            {% set db = node.database | upper %}
            {% set sc = node.schema | upper %}
            {% set tb = node.alias | upper if node.alias else node.name | upper %}
            {% set fqn_key = db ~ "." ~ sc ~ "." ~ tb %}
            {% do model_list.update({fqn_key: node.unique_id}) %}
        {% endfor %}

        {{ log("Models in dbt project: ", info=true) }}
        {% for key, val in model_list.items() %}
            {{ log(val, info=true) }}
        {% endfor %}

        {# --- 2. Get Large Tables --- #}
        {% set large_tables = get_large_tables(
            min_size_gb=min_size_gb,
            target_databases=target_databases,
            target_schemas=target_schemas
        ) %}

        {% if not large_tables or large_tables | length == 0 %}
            {{ log("No tables found matching size criteria.", info=true) }}
            {{ return("") }}
        {% endif %}

        {% set candidates = [] %}

        {# --- 3. Deep Dive --- #}
        {% for row in large_tables %}
            {% set db = row["TABLE_DATABASE"] %}
            {% set sc = row["TABLE_SCHEMA"] %}
            {% set tb = row["TABLE_NAME"] %}
            {% set fqn = db ~ "." ~ sc ~ "." ~ tb %}

            {# Check if this table is managed by dbt #}
            {% set dbt_model_id = model_list.get(fqn, none) %}


            {% if dbt_project_only and dbt_model_id is none %}
                {# Skip this iteration #}
            {% else %}

                {# Get Query Stats #}
                {% set stats_table = get_table_performance_stats(
                    db, sc, tb, lookback_days
                ) %}
                {% set stats = stats_table.rows[0] %}

                {% set select_count = ((stats["SELECT_COUNT"] or 0) | string) | int %}
                {% set dml_count = ((stats["DML_COUNT"] or 0) | string) | int %}
                {% set avg_exec_ms = (
                    (stats["AVG_EXECUTION_TIME_MS"] or 0) | string
                ) | float %}
                {% set avg_scanned = (
                    (stats["AVG_PARTITIONS_SCANNED"] or 0) | string
                ) | float %}
                {% set avg_total_parts = (
                    (stats["AVG_PARTITIONS_TOTAL"] or 0) | string
                ) | float %}

                {% set actual_partitions = avg_total_parts | int %}
                {% if actual_partitions == 0 %}
                    {% set actual_partitions = row["APPROX_MICROPARTITIONS"] | int %}
                {% endif %}

                {# --- 4. Scoring & Logic --- #}
                {% set is_candidate = false %}
                {% set base_score = 0 %}
                {% set safe_dml = 1 if dml_count == 0 else dml_count %}
                {% set query_ratio = select_count / safe_dml %}

                {# Query Volume * Execution Time * (Query/DML Ratio) #}
                {% if select_count > 0 %}
                    {% set base_score = (select_count * (avg_exec_ms / 1000)) + (
                        query_ratio * 10
                    ) %}

                    {# High Reads vs Writes #}
                    {% if query_ratio > 1 and row["SIZE_GB"] >= min_size_gb %}
                        {% set is_candidate = true %}
                    {% endif %}
                {% endif %}

                {# Calculate Avg Rows Per Parition #}
                {% set total_rows = row['ROW_COUNT'] | int %}
                {% set avg_rows_per_partition = 0 %}
                {% if actual_partitions > 0 %}
                    {% set avg_rows_per_partition = total_rows / actual_partitions %}
                {% endif %}

                {# 1. Calculate Ratio: Partitions vs Rows #}
                {% set partition_ratio_pct = 0 %}
                {% if total_rows > 0 %}
                    {% set partition_ratio_pct = (actual_partitions / total_rows) * 100 %}
                {% endif %}

                {# 2. Calculate Multiplier Based on Partition Percentage #}
                {# Ideally, this number is < 0.001%. If it rises above this, we multiply the score. #}
                
                {% set multiplier = 1 %}
                
                {# If partitions represent more than 0.01% of rows, start penalizing #}
                {% if partition_ratio_pct > 0.0001 %}
                    {# Use the ratio as the multiplier. e.g. 10% ratio = 10x score boost #}
                    {% set multiplier = partition_ratio_pct %}
                {% endif %}

                {% set final_score = base_score * multiplier %}

                {% do candidates.append(
                    {
                        "fqn": fqn,
                        "dbt_model": dbt_model_id,
                        "table_type": row['TABLE_TYPE'],
                        "is_candidate": is_candidate,
                        "score": final_score,
                        "size_gb": row["SIZE_GB"],
                        'row_count': total_rows,
                        "micropartitions": actual_partitions,
                        "avg_rows_per_partition": avg_rows_per_partition | round(2),
                        "avg_partitions_scanned": avg_scanned,
                        "select_count": select_count,
                        "dml_count": dml_count,
                        "avg_exec_sec": (avg_exec_ms / 1000) | round(2),
                    }
                ) %}
            
            {% endif %}

        {% endfor %}

        {% if preview_only %}
        {# --- 5. Output Results --- #}
        {% set sorted_candidates = candidates | sort(attribute="score", reverse=true) %}

        {{ log("\n--- Top 10 Clustering Candidates ---", info=true) }}
        {% for c in sorted_candidates %}
            {% if loop.index <= 10 %}
                {{ log("------------------------------------------------", info=true) }}
                {{ log("Table: " ~ c.fqn, info=true) }}
                {{ log("dbt Model: " ~ c.dbt_model, info=true) }}
                {{ log("Table Type: " ~ c.table_type, info=true) }}
                {{log("Score: " ~ c.score | int ~ " | Potential candidate? " ~ c.is_candidate, info=true)}}
                {{ log("Table Size: " ~ c.size_gb | int ~ " GB ", info=true) }}
                {{ log("Total rows: " ~ c.row_count, info=true) }}
                {{ log("Current Micropartitions: " ~ c.micropartitions, info=true) }}
                {{ log("Average Rows per Micropartition: " ~ c.avg_rows_per_partition, info=true)}}
                {{log("Average Partitions Scanned: " ~ c.avg_partitions_scanned, info=true) }}
                {{log("Usage: " ~ c.select_count ~ " SELECTs | " ~ c.dml_count ~ " DMLs (Ratio: " ~ (c.select_count / (c.dml_count + 1)) | round(1) ~ ")", info=true) }}
                {{ log("Average Query Duration: " ~ c.avg_exec_sec ~ "s", info=true) }}
            {% endif %}
        {% endfor %}
        {% else %}
            {{ log("Populating model clustering_table_candidates with results...", info=true)}}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro get_large_tables(min_size_gb=1024, target_databases=[], target_schemas=[]) %}
  {#--
    Scans Snowflake ACCOUNT_USAGE to find large tables across the entire account.
    Requires privileges on SNOWFLAKE database.
  --#}
  {% set sql %}
    select
        t.table_catalog as table_database,
        t.table_schema,
        t.table_name,
        sm.active_bytes as size_bytes,
        sm.active_bytes / power(1024, 3) as size_gb,
        t.row_count,
        t.clustering_key is not null as is_already_clustered,
        -- Calculate approximate partitions based on size if history is missing
        (sm.active_bytes / (16 * 1024 * 1024)) as approx_micropartitions,
        case 
            when t.table_type = 'MATERIALIZED VIEW' then 'Materialized View'
            when t.is_transient = 'YES' then 'Transient Table'
            else 'Permanent Table' 
        end as table_type
    from snowflake.account_usage.tables t
    join snowflake.account_usage.table_storage_metrics sm
    on t.table_catalog = sm.table_catalog
    and t.table_schema = sm.table_schema
    and t.table_name = sm.table_name
    where sm.active_bytes >= ({{ min_size_gb }} * 1024 * 1024 * 1024)
    and t.table_type in ('BASE TABLE', 'MATERIALIZED VIEW')
    and t.deleted is null -- Crucial: ACCOUNT_USAGE contains history of dropped tables
    and sm.deleted = FALSE
    
    {# --- Dynamic Database Filtering --- #}
    {% if target_databases and target_databases | length > 0 %}
      and t.table_catalog in (
        {% for db in target_databases %}
          '{{ db | upper }}'{% if not loop.last %},{% endif %}
        {% endfor %}
      )
    {% endif %}

    {# --- Dynamic Schema Filtering --- #}
    {% if target_schemas and target_schemas | length > 0 %}
      and t.table_schema in (
        {% for sc in target_schemas %}
          '{{ sc | upper }}'{% if not loop.last %},{% endif %}
        {% endfor %}
      )
    {% endif %}

    order by size_gb desc

    -- limit results for performance here
    limit 100
  {% endset %}

  {{ return(run_query(sql)) }}
{% endmacro %}



{% macro get_table_performance_stats(table_database, table_schema, table_name, lookback_days
) %}

    {% set fqn = table_database ~ "." ~ table_schema ~ "." ~ table_name %}

    {% set sql %}
    with query_stats as (
        select
            query_type,
            execution_time,
            partitions_scanned,
            partitions_total
        from snowflake.account_usage.query_history
        where start_time >= dateadd('day', -{{ lookback_days }}, current_timestamp())
        and query_text ilike '%{{ table_name }}%' 
        and execution_status = 'SUCCESS'
    )
    select
        count(case when query_type = 'SELECT' then 1 end) as select_count,
        count(case when query_type in ('INSERT', 'UPDATE', 'DELETE', 'MERGE') then 1 end) as dml_count,
        avg(case when query_type = 'SELECT' then execution_time else null end) as avg_execution_time_ms,
        avg(case when query_type = 'SELECT' then partitions_scanned else null end) as avg_partitions_scanned,
        avg(case when query_type = 'SELECT' then partitions_total else null end) as avg_partitions_total
    from query_stats
    {% endset %}

    {{ return(run_query(sql)) }}
{% endmacro %}
