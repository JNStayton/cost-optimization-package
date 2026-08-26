{% macro find_warehouse_sizing_recommendations(lookback_days=7, min_query_count=20, dml_threshold=0.35) %}

  {#--
    Recommends warehouse-level sizing actions for warehouses running dbt workloads.

    Evaluates each warehouse over the lookback window and produces one of:
      - Enable Gen2 (DML-heavy workload)
      - Enable multi-cluster or split workload (concurrency-bound)
      - Scale up single cluster (moderate concurrency bottleneck)
      - Scale down single cluster (oversized for the workload)
      - Stable (no sizing change recommended)

    The recommendation tree is intentionally ordered: Gen2 takes priority over MCW
    when DML is heavy, since switching generations can absorb concurrency pressure
    on its own and is usually cheaper than spinning up additional clusters.

    How to run:
      dbt run-operation find_warehouse_sizing_recommendations

    With custom args:
      dbt run-operation find_warehouse_sizing_recommendations \
        --args '{lookback_days: 14, min_query_count: 50, dml_threshold: 0.25}'

    [TODO] Persist results to a model so we can track recommendation history.
  --#}

  {% if execute %}

    {% set is_enterprise = var('snowflake_enterprise_edition', true) %}

    {{ log("--- Starting Warehouse Sizing Recommendation Analysis ---", info=true) }}
    {{ log("Criteria: warehouses with >= " ~ min_query_count ~ " successful dbt queries in last " ~ lookback_days ~ " days.", info=true) }}
    {{ log("Edition: " ~ ('Enterprise+' if is_enterprise else 'Standard') ~ " | Config-aware: yes", info=true) }}

    {# --- Fetch current warehouse config for gating --- #}
    {% set config_sql %}
        select
            warehouse_name,
            coalesce(resource_constraint, '') = 'STANDARD_GEN_2' as is_gen2,
            coalesce(cluster_count, 1) > 1 as is_multicluster,
            coalesce(warehouse_type, '') = 'ADAPTIVE' as is_adaptive
        from snowflake.account_usage.warehouse_events_history
        where event_name = 'WAREHOUSE_CONSISTENT'
            and timestamp >= dateadd('day', -90, current_timestamp())
        qualify row_number() over (partition by warehouse_name order by timestamp desc) = 1
    {% endset %}
    {% set config_results = run_query(config_sql) %}
    {% set wh_config = {} %}
    {% for row in config_results.rows %}
        {% do wh_config.update({row["WAREHOUSE_NAME"]: {
            "is_gen2": row["IS_GEN2"],
            "is_multicluster": row["IS_MULTICLUSTER"],
            "is_adaptive": row["IS_ADAPTIVE"]
        }}) %}
    {% endfor %}

    {% set performance_sql %}
      with dbt_sessions as (
          {{ dbt_cost_optimization_package.dbt_session_filter(lookback_days=lookback_days) }}
      ),

      dbt_query_stats as (
          select
              qh.warehouse_name,
              qh.warehouse_size,
              qh.total_elapsed_time,
              qh.queued_overload_time,
              qh.query_type
          from
              snowflake.account_usage.query_history qh
          inner join
              dbt_sessions s on qh.session_id = s.session_id
          where
              qh.start_time >= dateadd(day, -{{ lookback_days }}, current_timestamp())
              and qh.execution_status = 'SUCCESS'
              and qh.warehouse_size is not null
      ),

      warehouse_summary as (
          select
              warehouse_name,
              warehouse_size,
              count(*) as total_dbt_queries,
              round(count(case when query_type in ('MERGE', 'UPDATE', 'DELETE', 'INSERT') then 1 end) * 1.0 / count(*), 4) as dml_ratio,
              round(count(case when query_type in ('MERGE', 'UPDATE', 'DELETE', 'INSERT') then 1 end) * 100.0 / count(*), 1) as dml_percentage,
              round(median(total_elapsed_time / 1000), 2) as median_execution_sec,
              round(median(queued_overload_time / 1000), 2) as median_overload_sec,
              coalesce(
                  round(
                      median(queued_overload_time / 1000)
                      / nullif(median(total_elapsed_time / 1000), 0),
                      4
                  ),
                  0
              ) as overload_to_execution_ratio
          from
              dbt_query_stats
          group by 1, 2
          having count(*) >= {{ min_query_count }}
      )

      {#-
        recommendation_key encodes the threshold logic so we never compare
        Snowflake Decimal column values to Jinja floats — minijinja's strict
        cross-type comparison was returning truthy regardless of values.

        thresholds (in seconds, except dml/ratio):
          dml_threshold (param):              gen2 trigger if dml_ratio exceeds this
          high_overload_sec = 5:              mcw trigger floor
          mcw_overload_ratio_threshold = 0.1: overload must also be >10% of execution
          moderate_overload_sec = 1:          scale-up trigger floor
          scale_down_max_overload_sec = 0.5:  scale-down requires overload below this
          scale_down_max_execution_sec = 5:   scale-down requires execution below this
      -#}
      select
          warehouse_name,
          warehouse_size,
          total_dbt_queries,
          dml_percentage,
          median_execution_sec,
          median_overload_sec,
          case
              when dml_ratio > {{ dml_threshold }} then 'gen2'
              when median_overload_sec > 5 and overload_to_execution_ratio > 0.1 then 'mcw'
              when median_overload_sec > 1 and median_overload_sec <= 5 then 'scale_up'
              when median_execution_sec < 5 and median_overload_sec < 0.5 then 'scale_down'
              else 'stable'
          end as recommendation_key
      from
          warehouse_summary
      order by
          total_dbt_queries desc
    {% endset %}

    {% set results = run_query(performance_sql) %}

    {% if results.rows | length == 0 %}
        {{ log("No warehouses met the criteria. Try lowering min_query_count or extending lookback_days.", info=true) }}
    {% else %}

        {{ log("Evaluated " ~ results.rows | length ~ " warehouse(s).", info=true) }}
        {{ log("", info=true) }}

        {% set dml_threshold_pct = (dml_threshold * 100) | round(1) %}
        {% set recommendations = [] %}

        {% for row in results.rows %}
            {% set warehouse_name = row["WAREHOUSE_NAME"] %}
            {% set warehouse_size = row["WAREHOUSE_SIZE"] %}
            {% set total_queries = row["TOTAL_DBT_QUERIES"] %}
            {% set dml_percentage = row["DML_PERCENTAGE"] %}
            {% set median_execution_sec = row["MEDIAN_EXECUTION_SEC"] %}
            {% set median_overload_sec = row["MEDIAN_OVERLOAD_SEC"] %}
            {% set recommendation_key = row["RECOMMENDATION_KEY"] %}

            {% set recommendation = none %}
            {% set reason = none %}
            {% set severity = 'info' %}

            {# Look up current config for this warehouse #}
            {% set wh_cfg = wh_config.get(warehouse_name, {"is_gen2": false, "is_multicluster": false, "is_adaptive": false}) %}

            {% if wh_cfg.is_adaptive %}
                {% set recommendation = 'Stable — adaptive warehouse (self-optimizing)' %}
                {% set reason = 'Adaptive warehouses auto-scale per query. No manual sizing action needed.' %}
            {% elif recommendation_key == 'gen2' and wh_cfg.is_gen2 %}
                {% set recommendation = 'Already on Gen2 — DML workload detected but Gen2 is active' %}
                {% set reason = 'DML ratio of ' ~ dml_percentage ~ '% exceeds threshold but Gen2 is already enabled. Consider query optimization or workload splitting.' %}
            {% elif recommendation_key == 'gen2' %}
                {% set recommendation = 'Enable Gen2 warehouse (DML-heavy workload)' %}
                {% set reason = 'DML ratio of ' ~ dml_percentage ~ '% exceeds the ' ~ dml_threshold_pct ~ '% threshold; Gen2 is optimized for DML.' %}
                {% set severity = 'warn' %}
            {% elif recommendation_key == 'mcw' and wh_cfg.is_multicluster %}
                {% set recommendation = 'Already multi-cluster — review scaling policy or increase max_cluster_count' %}
                {% set reason = 'Median overload of ' ~ median_overload_sec ~ 's exceeds 5s but warehouse is already multi-cluster. Review scaling policy or increase max clusters.' %}
                {% set severity = 'warn' %}
            {% elif recommendation_key == 'mcw' and is_enterprise %}
                {% set recommendation = 'Enable multi-cluster (Enterprise+) or split workload across warehouses' %}
                {% set reason = 'Median overload of ' ~ median_overload_sec ~ 's exceeds 5s and is >10% of median execution time (' ~ median_execution_sec ~ 's).' %}
                {% set severity = 'warn' %}
            {% elif recommendation_key == 'mcw' and not is_enterprise %}
                {% set recommendation = 'Split workload across dedicated warehouses (Standard edition)' %}
                {% set reason = 'Median overload of ' ~ median_overload_sec ~ 's exceeds 5s. Multi-cluster not available on Standard edition — split by workload type.' %}
                {% set severity = 'warn' %}
            {% elif recommendation_key == 'scale_up' %}
                {% set recommendation = 'Scale up single cluster' %}
                {% set reason = 'Median overload of ' ~ median_overload_sec ~ 's indicates a moderate concurrency bottleneck.' %}
            {% elif recommendation_key == 'scale_down' %}
                {% set recommendation = 'Scale down single cluster' %}
                {% set reason = 'Median execution of ' ~ median_execution_sec ~ 's with negligible overload — warehouse appears oversized for the workload.' %}
            {% else %}
                {% set recommendation = 'Stable — no sizing change recommended' %}
                {% set reason = 'Metrics within healthy range. Consider SQL-level optimization or schedule tuning if performance is unexpected.' %}
            {% endif %}

            {% do recommendations.append({
                'warehouse_name': warehouse_name,
                'warehouse_size': warehouse_size,
                'total_queries': total_queries,
                'dml_percentage': dml_percentage,
                'median_execution_sec': median_execution_sec,
                'median_overload_sec': median_overload_sec,
                'recommendation': recommendation,
                'reason': reason,
                'severity': severity
            }) %}
        {% endfor %}

        {# warns first, then by total dbt queries desc within each severity bucket #}
        {% set warns = recommendations | selectattr('severity', 'equalto', 'warn') | list %}
        {% set infos = recommendations | selectattr('severity', 'equalto', 'info') | list %}
        {% set sorted_recs = warns + infos %}

        {{ log("--- Recommendations ---", info=true) }}

        {% for r in sorted_recs %}
            {% if loop.index > 20 %}{% break %}{% endif %}

            {{ dbt_cost_optimization_package.log_recommendation(
                title='Warehouse: ' ~ r.warehouse_name ~ ' (' ~ r.warehouse_size ~ ')',
                recommendation=r.recommendation,
                reason=r.reason,
                severity=r.severity,
                metrics={
                    'total_dbt_queries': r.total_queries,
                    'dml_percentage': r.dml_percentage ~ '%',
                    'median_execution_sec': r.median_execution_sec,
                    'median_overload_sec': r.median_overload_sec
                }
            ) }}
        {% endfor %}

    {% endif %}

  {% endif %}

{% endmacro %}
