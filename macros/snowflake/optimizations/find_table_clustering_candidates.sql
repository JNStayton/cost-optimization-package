{% macro find_table_clustering_candidates(lookback_days=7, ignore_table_size=false, dbt_project_only=true, target_databases=[], target_schemas=[]) %}

  {#--
    Identifies Snowflake tables that may benefit from clustering, scored by
    observed scan inefficiency, query volume, and read/write ratio.

    V3 scoring formula:
      score = total_read_time * partition_scan_ratio * read_heaviness_boost

      - total_read_time:       select_count * avg_exec_sec
      - partition_scan_ratio:  partitions_scanned / (partitions_scanned + partitions_pruned)
                               From TABLE_QUERY_PRUNING_HISTORY (per-table, not per-query).
                               Falls back to 0.5 when no pruning data available.
      - read_heaviness_boost:  Stepped multiplier (1-5x) based on read:write ratio.

    This formula is warehouse-size independent: the scan ratio measures physical
    pruning behavior per table, not wall-clock time on a particular warehouse size.

    Output tiers (recommendation_tier):
      - High impact:     scan_ratio >= 0.5 AND score >= 1000
      - Moderate impact: scan_ratio >= 0.5 AND score < 1000
      - Low impact:      scan_ratio 0.2 - 0.5
      - Healthy:         scan_ratio < 0.2 (pruning effective)
      - Write-heavy:     query_ratio <= 1
      - No read activity: select_count = 0

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

                {# --- Get per-table pruning stats --- #}
                {% set pruning_table = get_table_pruning_stats(
                    db, sc, tb, lookback_days
                ) %}
                {% set pruning = pruning_table.rows[0] %}

                {% set total_scanned = ((pruning["TOTAL_PARTITIONS_SCANNED"] or 0) | string) | int %}
                {% set total_pruned = ((pruning["TOTAL_PARTITIONS_PRUNED"] or 0) | string) | int %}
                {% set select_count = ((pruning["TOTAL_QUERY_COUNT"] or 0) | string) | int %}
                {% set avg_exec_ms = (
                    (pruning["AVG_EXECUTION_TIME_MS"] or 0) | string
                ) | float %}

                {# --- Get DML count for read:write ratio --- #}
                {% set dml_table = get_table_dml_count(
                    db, sc, tb, lookback_days
                ) %}
                {% set dml_count = ((dml_table.rows[0]["DML_COUNT"] or 0) | string) | int %}

                {# --- Micropartitions from pruning history or get_large_tables fallback --- #}
                {% set approx_partitions = row["APPROX_MICROPARTITIONS"] | string | int %}
                {% set actual_partitions = total_scanned + total_pruned %}
                {% if actual_partitions == 0 %}
                    {% set actual_partitions = approx_partitions %}
                {% endif %}

                {# --- 4. V3 Scoring (using per-table pruning data) --- #}
                {% set avg_exec_sec = avg_exec_ms / 1000 %}
                {% set safe_dml = 1 if dml_count == 0 else dml_count %}
                {% set query_ratio = select_count / safe_dml %}

                {# Per-table scan ratio: scanned / (scanned + pruned). Fallback 0.5 when no data #}
                {% set scan_ratio = 0.5 %}
                {% if (total_scanned + total_pruned) > 0 %}
                    {% set scan_ratio = total_scanned / (total_scanned + total_pruned) %}
                {% endif %}

                {# Read-heaviness boost: stepped multiplier #}
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

                {# Recommendation tier #}
                {% set recommendation_tier = 'Healthy' %}
                {% if select_count == 0 %}
                    {% set recommendation_tier = 'No read activity' %}
                {% elif query_ratio <= 1 %}
                    {% set recommendation_tier = 'Write-heavy' %}
                {% elif scan_ratio >= 0.5 and score >= 1000 %}
                    {% set recommendation_tier = 'High impact' %}
                {% elif scan_ratio >= 0.5 %}
                    {% set recommendation_tier = 'Moderate impact' %}
                {% elif scan_ratio >= 0.2 %}
                    {% set recommendation_tier = 'Low impact' %}
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
                    {% set reason = 'Pruning already effective (scanning ' ~ _scan_pct ~ '% of partitions).' %}
                {% else %}
                    {% set reason = 'Below size threshold or insufficient data.' %}
                {% endif %}

                {% do candidates.append(
                    {
                        "fqn": fqn,
                        "dbt_model": dbt_model_id,
                        "table_type": row['TABLE_TYPE'],
                        "is_candidate": is_candidate,
                        "recommendation_tier": recommendation_tier,
                        "has_pruning_data": (total_scanned + total_pruned) > 0,
                        "score": score,
                        "size_gb": row["SIZE_GB"],
                        "row_count": total_rows,
                        "micropartitions": actual_partitions,
                        "avg_rows_per_partition": avg_rows_per_partition,
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
                    recommendation=c.recommendation_tier ~ " | Score: " ~ c.score | int,
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
    {% endif %}
{% endmacro %}