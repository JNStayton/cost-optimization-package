{% macro find_table_clustering_candidates_v2(lookback_days=7, ignore_table_size=false, dbt_project_only=true, target_databases=[], target_schemas=[], preview_only=true) %}

  {#--
    V2 of find_table_clustering_candidates with a simplified scoring formula.

    Differences from v1:
      v1 score: (select_count * avg_exec_sec) + (query_ratio * 10), multiplied by
                partition_ratio_pct when > 0.0001.
      v2 score: select_count * avg_exec_sec — total compute time spent reading
                the table.

    Rationale: v1's additive query_ratio * 10 term is orders of magnitude smaller
    than the first term in almost every practical case, so it contributes little
    to the ranking but adds complexity. The partition_ratio multiplier is also
    unbounded, which can produce extreme scores for tables with many small
    partitions and crowd out candidates that genuinely matter.

    V2 uses just the direct measure of optimization opportunity (total reader
    time on the table). Partition ratio and query ratio are surfaced as displayed
    metrics, not baked into the score.

    Other than scoring, v2 is identical to v1: same candidacy filter
    (query_ratio > 1), same dbt graph join, same SQL helpers, same output format.

    Run v1 and v2 side by side and compare top-10 outputs to decide which scoring
    surfaces more useful candidates for your workload.

    [TODO] Creates/updates a model with the information for historical analysis.

    How to run:
    dbt run-operation find_table_clustering_candidates_v2

    How to run with custom args:
    dbt run-operation find_table_clustering_candidates_v2 --args '{lookback_days: 10, target_databases: ['db_1', 'db_2']}'
  --#}

    {% if ignore_table_size %}
        {% set min_size_gb = 1 %}
    {% else %}
        {% set min_size_gb = 100 %}
    {% endif %}

    {% if execute %}

        {{ log("--- Starting Clustering Candidate Analysis (v2: simplified score) ---", info=true) }}
        {{ log("Criteria: Table > " ~ min_size_gb ~ " GB | Lookback: " ~ lookback_days ~ " days | Must be dbt model: " ~ dbt_project_only, info=true) }}
        {{ log("Score formula: select_count * avg_exec_sec (total time spent reading the table).", info=true) }}

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

        {# --- 2. Get Large Tables (already-clustered tables filtered out by helper) --- #}
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

            {% set dbt_model_id = model_list.get(fqn, none) %}

            {% if dbt_project_only and dbt_model_id is none %}
                {# Skip this iteration #}
            {% else %}

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

                {# --- 4. v2 Scoring & Candidacy --- #}
                {% set is_candidate = false %}
                {% set score = 0 %}
                {% set safe_dml = 1 if dml_count == 0 else dml_count %}
                {% set query_ratio = select_count / safe_dml %}

                {# v2 score: total time spent reading the table — direct measure of optimization opportunity. #}
                {% if select_count > 0 %}
                    {% set score = select_count * (avg_exec_ms / 1000) %}
                    {% if query_ratio > 1 %}
                        {% set is_candidate = true %}
                    {% endif %}
                {% endif %}

                {% set total_rows = row['ROW_COUNT'] | int %}
                {% set avg_rows_per_partition = 0 %}
                {% if actual_partitions > 0 %}
                    {% set avg_rows_per_partition = total_rows / actual_partitions %}
                {% endif %}

                {# v2: partition_ratio_pct is a displayed metric only, not a score multiplier. #}
                {% set partition_ratio_pct = 0 %}
                {% if total_rows > 0 %}
                    {% set partition_ratio_pct = (actual_partitions / total_rows) * 100 %}
                {% endif %}

                {% do candidates.append(
                    {
                        "fqn": fqn,
                        "dbt_model": dbt_model_id,
                        "table_type": row['TABLE_TYPE'],
                        "is_candidate": is_candidate,
                        "score": score,
                        "size_gb": row["SIZE_GB"],
                        'row_count': total_rows,
                        "micropartitions": actual_partitions,
                        "avg_rows_per_partition": avg_rows_per_partition | round(2),
                        "avg_partitions_scanned": avg_scanned,
                        "partition_ratio_pct": partition_ratio_pct | round(4),
                        "select_count": select_count,
                        "dml_count": dml_count,
                        "query_ratio": query_ratio | round(2),
                        "avg_exec_sec": (avg_exec_ms / 1000) | round(2),
                    }
                ) %}

            {% endif %}

        {% endfor %}

        {% if preview_only %}
        {# --- 5. Output Results --- #}
        {% set sorted_candidates = candidates | sort(attribute="score", reverse=true) %}

        {{ log("\n--- Top 10 Clustering Candidates (v2) ---", info=true) }}
        {% for c in sorted_candidates %}
            {% if loop.index <= 10 %}
                {{ log("------------------------------------------------", info=true) }}
                {{ log("Table: " ~ c.fqn, info=true) }}
                {{ log("dbt Model: " ~ c.dbt_model, info=true) }}
                {{ log("Table Type: " ~ c.table_type, info=true) }}
                {{ log("Score: " ~ c.score | int ~ " | Potential candidate? " ~ c.is_candidate, info=true) }}
                {{ log("Table Size: " ~ c.size_gb | int ~ " GB ", info=true) }}
                {{ log("Total rows: " ~ c.row_count, info=true) }}
                {{ log("Current Micropartitions: " ~ c.micropartitions, info=true) }}
                {{ log("Average Rows per Micropartition: " ~ c.avg_rows_per_partition, info=true) }}
                {{ log("Average Partitions Scanned: " ~ c.avg_partitions_scanned, info=true) }}
                {{ log("Partition Ratio (partitions/rows * 100): " ~ c.partition_ratio_pct ~ "%", info=true) }}
                {{ log("Usage: " ~ c.select_count ~ " SELECTs | " ~ c.dml_count ~ " DMLs (Ratio: " ~ c.query_ratio ~ ")", info=true) }}
                {{ log("Average Query Duration: " ~ c.avg_exec_sec ~ "s", info=true) }}
                {% if c.is_candidate and c.dbt_model %}
                    {{ log("Next step: run `dbt run-operation suggest_clustering_keys --args '{model_name: <your_model_name>}'` for column-level clustering key recommendations.", info=true) }}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% else %}
            {{ log("Populating model clustering_table_candidates with results...", info=true)}}
        {% endif %}
    {% endif %}
{% endmacro %}
