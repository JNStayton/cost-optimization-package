{% macro probe_unique_key_candidates() %}

  {#--
    Post-hook for fct_snowflake__incremental_config_recommendations.

    For each candidate table, probes the unique_key_candidates array using
    APPROX_COUNT_DISTINCT to find the first column whose cardinality is
    approximately equal to the row count (>= 95% threshold, accounting for
    HyperLogLog's error margin). All candidate columns for a table are probed
    in a single scan to minimise compute cost.

    When a likely unique key is confirmed it updates:
      - likely_unique_key       — the confirmed column name
      - validate_uniqueness_sql — re-pointed to the confirmed column
      - dbt_config_template     — unique_key parameter swapped to confirmed column

    When no single-column unique key is found, likely_unique_key stays null.
    This correctly signals that the table requires a composite key or a
    dedicated surrogate key column.

    Variables:
      incremental_unique_key_probe_threshold (default 0.95)
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set threshold = var('incremental_unique_key_probe_threshold', 0.95) %}

    {{ log("probe_unique_key_candidates: starting uniqueness probe...", info=true) }}

    {# Fetch every candidate table and its unique key candidates as a CSV string.
       array_to_string avoids having to parse a Snowflake ARRAY in Python/Jinja. #}
    {% set candidates_sql %}
      select
        table_fqn,
        best_unique_key,
        array_to_string(unique_key_candidates, ',') as candidates_csv
      from {{ this }}
      where unique_key_candidates is not null
        and array_size(unique_key_candidates) > 0
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for row in candidates %}

        {% set table_fqn     = row['TABLE_FQN'] %}
        {% set best_conv_key = row['BEST_UNIQUE_KEY'] %}
        {% set candidates_csv = row['CANDIDATES_CSV'] %}
        {% set candidate_cols = candidates_csv.split(',') %}

        {{ log("probe_unique_key_candidates: probing " ~ (candidate_cols | length) ~ " candidate(s) for " ~ table_fqn, info=true) }}

        {# Scan the table once — one APPROX_COUNT_DISTINCT per candidate column #}
        {% set probe_sql %}
          select
            count(*) as total_rows
            {% for col in candidate_cols %}
              , approx_count_distinct({{ adapter.quote(col) }}) as col_{{ loop.index }}_distinct
            {% endfor %}
          from {{ table_fqn }}
        {% endset %}

        {% set probe_result = run_query(probe_sql) %}
        {% set total_rows = probe_result.rows[0][0] | int %}

        {# Walk candidates in rank order; stop at the first likely-unique column.
           namespace() is required to mutate a variable inside a Jinja for loop. #}
        {% set ns = namespace(confirmed_key=none) %}

        {% for col in candidate_cols %}
          {% if ns.confirmed_key is none %}
            {% set approx_distinct = probe_result.rows[0][loop.index] | int %}
            {% if total_rows > 0 and (approx_distinct / total_rows) >= threshold %}
              {% set ns.confirmed_key = col | lower %}
            {% endif %}
          {% endif %}
        {% endfor %}

        {% if ns.confirmed_key is not none %}

          {% set new_validate_sql = 'select count(*) = count(distinct ' ~ ns.confirmed_key ~ ') as is_unique from ' ~ table_fqn | lower %}
          {% set old_key_in_template = (best_conv_key | lower) if best_conv_key else '<unique_key>' %}

          {% set update_sql %}
            update {{ this }}
            set
              likely_unique_key       = '{{ ns.confirmed_key }}',
              validate_uniqueness_sql = '{{ new_validate_sql }}',
              dbt_config_template     = replace(
                dbt_config_template,
                'unique_key=''' || '{{ old_key_in_template }}' || '''',
                'unique_key=''' || '{{ ns.confirmed_key }}' || ''''
              )
            where table_fqn = '{{ table_fqn }}'
          {% endset %}

          {% do run_query(update_sql) %}
          {{ log("probe_unique_key_candidates: confirmed '" ~ ns.confirmed_key ~ "' as likely unique key for " ~ table_fqn, info=true) }}

        {% else %}
          {{ log("probe_unique_key_candidates: no single-column unique key found for " ~ table_fqn ~ " — composite key likely needed", info=true) }}
        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("probe_unique_key_candidates: no candidates with unique key candidates found, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
