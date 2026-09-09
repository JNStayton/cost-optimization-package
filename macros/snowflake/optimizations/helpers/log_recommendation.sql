{% macro log_recommendation(title, recommendation, reason, metrics={}, severity='info') %}

  {#--
    Formats a single optimization recommendation as a uniform block in the dbt log.
    Used by the find_* recommendation macros so their output stays consistent.

    Args:
      title:          subject of the recommendation (e.g. "Warehouse: PROD_WH" or
                      "Model: db.schema.tbl"). Becomes the header line.
      recommendation: what action to take (e.g. "Scale down single cluster").
      reason:         why this recommendation was generated (e.g. "Avg load 12%
                      over last 7 days, no queue time observed").
      metrics:        optional dict of {label: value} pairs printed under "Metrics:".
                      Values are stringified as-is; round/format them at the call site.
      severity:       'info' (default) or 'warn'. Anything other than 'info' is
                      shown as a bracketed prefix on the title line (e.g. "[WARN] ...").

    Output shape:

      [WARN] Warehouse: PROD_WH
        - Recommendation: Scale up single cluster
        - Reason: Median overload 12s on M warehouse with median execution 80s
        - Metrics:
            total_queries: 412
            median_execution_sec: 80.2
            median_overload_sec: 12.1
      ---
  --#}

  {% set severity_tag = '[' ~ severity | upper ~ '] ' if severity and severity != 'info' else '' %}

  {{ log(severity_tag ~ title, info=true) }}
  {{ log("  - Recommendation: " ~ recommendation, info=true) }}
  {{ log("  - Reason: " ~ reason, info=true) }}

  {% if metrics %}
    {{ log("  - Metrics:", info=true) }}
    {% for label, value in metrics.items() %}
      {{ log("      " ~ label ~ ": " ~ value, info=true) }}
    {% endfor %}
  {% endif %}

  {{ log("---", info=true) }}

{% endmacro %}
