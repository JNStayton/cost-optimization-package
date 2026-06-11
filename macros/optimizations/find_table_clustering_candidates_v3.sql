{% macro find_table_clustering_candidates_v3(lookback_days=7, ignore_table_size=false, dbt_project_only=true, target_databases=[], target_schemas=[], preview_only=true) %}

  {#--
    Identifies Snowflake tables that may benefit from clustering, scored by
    observed scan inefficiency, query volume, and read/write ratio.

    V3 scoring formula:
      score = total_read_time * partition_scan_ratio * read_heaviness_boost

      - total_read_time:       select_count * avg_exec_sec
      - partition_scan_ratio:  avg_partitions_scanned / avg_partitions_total (0-1)
                               Falls back to 0.5 when no partition stats available.
      - read_heaviness_boost:  tiered multiplier based on query_to_dml_ratio
                               to reward read-heavy tables without relying on
                               unsupported Jinja math filters.

    This formula is warehouse-size independent: the scan ratio measures physical
    pruning behavior, not wall-clock time on a particular warehouse size.

    A table is flagged as is_candidate when ALL of:
      1. select_count > 0 (actively read)
      2. query_to_dml_ratio > 1 (read-heavy)
      3. table size >= min_size_gb
      4. partition_scan_ratio > 0.5 (scanning >50% of partitions — poor pruning)

    How to run:
      dbt run-operation find_table_clustering_candidates_v3

    With custom args:
      dbt run-operation find_table_clustering_candidates_v3 --args '{lookback_days: 14, ignore_table_size: true}'
  --#}

    {% if ignore_table_size %}
        {% set min_size_gb = 1 %}
    {% else %}
        {% set min_size_gb = 100 %}
    {% endif %}

    {% if execute %}

        {{ log("--- Clustering Candidate Analysis (V3 scoring) ---", info=true) }}
        {{ log("Criteria: Table >= " ~ min_size_gb ~ " GB | Lookback: " ~ lookback_days ~ " days | dbt models only: " ~ dbt_project_only, info=true) }}
        {{ log("Score: total_read_time * partition_scan_ratio * tiered_read_heaviness_boost", info=true) }}

        {# --- 1. Build dbt Model List --- #}
        {% set model_list = {} %}
        {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            {% set db = node.database | upper %}
            {% set sc = node.schema | upper %}
            {% set node_identifier = node.alias if node.alias else node.name %}
            {% set tb = node_identifier | upper %}
            {% set fqn_key = db ~ "." ~ sc ~ "." ~ tb %}
            {% do model_list.update({fqn_key: node.unique_id}) %}
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

        {# --- 3. Evaluate Each Table --- #}
        {% for row in large_tables %}
            {% set db = row["TABLE_DATABASE"] %}
            {% set sc = row["TABLE_SCHEMA"] %}
            {% set tb = row["TABLE_NAME"] %}
            {% set fqn = db ~ "." ~ sc ~ "." ~ tb %}

            {% set dbt_model_id = model_list.get(fqn, none) %}

            {% if dbt_project_only and dbt_model_id is none %}
                {# Skip — not a dbt model #}
            {% else %}

                {% set stats_table = get_table_performance_stats(
                    db, sc, tb, lookback_days
                ) %}
                {% set stats = stats_table.rows[0] %}

                {% set select_count = ((stats["SELECT_COUNT"] or 0) | string) | int %}
                {% set dml_count = ((stats["DML_COUNT"] or 0) | string) | int %}
                {% set avg_exec_ms = (((stats["AVG_EXECUTION_TIME_MS"] or 0) | string) | float) %}
                {% set avg_scanned = (((stats["AVG_PARTITIONS_SCANNED"] or 0) | string) | float) %}
                {% set avg_total_parts = (((stats["AVG_PARTITIONS_TOTAL"] or 0) | string) | float) %}

                {% set actual_partitions = avg_total_parts | int %}
                {% if actual_partitions == 0 %}
                    {% set actual_partitions = row["APPROX_MICROPARTITIONS"] | string | int %}
                {% endif %}

                {# --- 4. V3 Scoring --- #}
                {% set avg_exec_sec = avg_exec_ms / 1000 %}
                {% set safe_dml = 1 if dml_count == 0 else dml_count %}
                {% set query_ratio = select_count / safe_dml %}

                {# Partition scan ratio: 0-1, fallback to 0.5 if no data #}
                {% set scan_ratio = 0.5 %}
                {% if avg_scanned > 0 and avg_total_parts > 0 %}
                    {% set scan_ratio = avg_scanned / avg_total_parts %}
                {% endif %}

                {# Read-heaviness boost: stepped multiplier instead of log math.
                   Keeps read-heavy tables ranked higher while staying Fusion-safe. #}
                {% set read_boost = 1 %}
                {% if query_ratio >= 20 %}
                    {% set read_boost = 5 %}
                {% elif query_ratio >= 10 %}
                    {% set read_boost = 4 %}
                {% elif query_ratio >= 5 %}
                    {% set read_boost = 3 %}
                {% elif query_ratio >= 2 %}
                    {% set read_boost = 2 %}
                {% endif %}

                {# V3 score: total_read_time * scan_ratio * read_boost #}
                {% set score = 0 %}
                {% set is_candidate = false %}
                {% if select_count > 0 %}
                    {% set score = select_count * avg_exec_sec * scan_ratio * read_boost %}
                    {% if query_ratio > 1 and scan_ratio > 0.5 %}
                        {% set is_candidate = true %}
                    {% endif %}
                {% endif %}

                {# Avg rows per partition #}
                {% set total_rows = row['ROW_COUNT'] | string | int %}
                {% set avg_rows_per_partition = 0 %}
                {% if actual_partitions > 0 %}
                    {% set avg_rows_per_partition = total_rows / actual_partitions %}
                {% endif %}

                {# Recommendation reason #}
                {% set _scan_pct = (scan_ratio * 100) | int %}
                {% set reason = '' %}
                {% if is_candidate %}
                    {% set reason = 'Queries scan ' ~ _scan_pct ~ '% of ' ~ actual_partitions ~ ' micropartitions. ' ~ select_count ~ ' reads at ' ~ avg_exec_sec ~ 's avg. Ratio: ' ~ query_ratio ~ ':1. Clustering on filtered columns would reduce scan overhead.' %}
                {% elif select_count == 0 %}
                    {% set reason = 'No read activity in lookback window.' %}
                {% elif query_ratio <= 1 %}
                    {% set reason = 'Write-heavy — clustering ROI unclear.' %}
                {% elif scan_ratio <= 0.5 %}
                    {% set reason = 'Pruning already effective (scanning <50% of partitions).' %}
                {% else %}
                    {% set reason = 'Below size threshold or insufficient data.' %}
                {% endif %}

                {% do candidates.append(
                    {
                        "fqn": fqn,
                        "dbt_model": dbt_model_id,
                        "table_type": row['TABLE_TYPE'],
                        "is_candidate": is_candidate,
                        "score": score,
                        "size_gb": row["SIZE_GB"],
                        "row_count": total_rows,
                        "micropartitions": actual_partitions,
                        "avg_rows_per_partition": avg_rows_per_partition,
                        "avg_partitions_scanned": avg_scanned,
                        "scan_ratio_pct": (scan_ratio * 100),
                        "select_count": select_count,
                        "dml_count": dml_count,
                        "query_ratio": query_ratio,
                        "avg_exec_sec": avg_exec_sec,
                        "read_boost": read_boost,
                        "reason": reason,
                    }
                ) %}

            {% endif %}
        {% endfor %}

        {% if preview_only %}
        {# --- 5. Output Results --- #}
        {% set sorted_candidates = candidates | sort(attribute="score", reverse=true) %}

        {{ log("\n--- Top 10 Clustering Candidates ---", info=true) }}

        {% if sorted_candidates | length == 0 %}
            {{ log("No clustering candidates found.", info=true) }}
            {{ log("", info=true) }}
            {{ log("Large tables were found but none qualified as candidates.", info=true) }}
            {{ log("Candidate criteria: read activity > 0, read:write ratio > 1, scan ratio > 50%.", info=true) }}
            {{ log("", info=true) }}
            {{ log("Suggestions:", info=true) }}
            {{ log("  - Try extending lookback_days (e.g., --args '{lookback_days: 14}')", info=true) }}
            {{ log("  - Set ignore_table_size: true to include smaller tables", info=true) }}
            {{ log("  - Set dbt_project_only: false to scan all tables, not just dbt models", info=true) }}
        {% endif %}

        {% for c in sorted_candidates %}
            {% if loop.index <= 10 %}
                {{ log("", info=true) }}
                {{ log_recommendation(
                    title="Table: " ~ c.fqn,
                    recommendation="Candidate: " ~ c.is_candidate ~ " | Score: " ~ c.score | int,
                    reason=c.reason,
                    metrics={
                        "table_size_gb": c.size_gb | int,
                        "total_rows": c.row_count,
                        "micropartitions": c.micropartitions,
                        "avg_rows_per_partition": c.avg_rows_per_partition,
                        "scan_ratio": c.scan_ratio_pct ~ "%",
                        "select_count": c.select_count,
                        "dml_count": c.dml_count,
                        "read_write_ratio": c.query_ratio,
                        "avg_exec_sec": c.avg_exec_sec,
                        "read_boost": c.read_boost ~ "x"
                    },
                    severity='warn' if c.is_candidate else 'info'
                ) }}
                {% if c.is_candidate and c.dbt_model %}
                    {{ log("  Next: dbt run-operation suggest_clustering_keys --args '{model_name: <model>}'", info=true) }}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% else %}
            {{ log("Populating model with results...", info=true) }}
        {% endif %}
    {% endif %}
{% endmacro %}