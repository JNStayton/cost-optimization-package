{% macro extract_query_operator_columns() %}

  {#--
    Populates int_snowflake__query_operator_columns with exact filter/join column
    usage parsed from query plan operators via GET_QUERY_OPERATOR_STATS.

    Runs as a post-hook on fct_snowflake__table_clustering_candidates, after
    refresh_column_cardinality. Processes representative queries for the top N
    candidate tables to build precise column-level filter/join evidence.

    GET_QUERY_OPERATOR_STATS constraints:
      - Requires literal query_id (no variable or set-based input)
      - 14-day data retention
      - Returns operator_type, operator_attributes (JSON with conditions)

    Parsing approach (all in SQL):
      - Filter operators: extract filter_condition, match known column names via ILIKE
      - Join operators: extract equality_join_condition, match known column names via ILIKE
      - Column names are matched against int_snowflake__table_columns for the target table

    Variables:
      clustering_key_operator_analysis_table_limit (default 10) — max tables to analyze
      clustering_key_operator_queries_per_table    (default 20) — representative queries per table
  --#}

  {% if execute and target.type == 'snowflake' %}

    {% set table_limit = var('clustering_key_operator_analysis_table_limit', 10) %}
    {% set queries_per_table = var('clustering_key_operator_queries_per_table', 20) %}

    {% set candidates_table = ref('fct_snowflake__table_clustering_candidates') %}
    {% set operator_table = ref('int_snowflake__query_operator_columns') %}
    {% set col_access_table = ref('int_snowflake__column_query_access') %}
    {% set table_columns_ref = ref('int_snowflake__table_columns') %}
    {% set query_history_table = ref('int_snowflake__query_history') %}

    {{ log("extract_query_operator_columns: fetching top " ~ table_limit ~ " candidates...", info=true) }}

    {# Step 1: Get top candidate tables #}
    {% set candidates_sql %}
      select
          table_fqn,
          database_name,
          schema_name,
          table_name
      from {{ candidates_table }}
      where is_candidate = true
          and snapshot_date = current_date()
      qualify row_number() over (order by score desc) <= {{ table_limit }}
    {% endset %}

    {% set candidates = run_query(candidates_sql) %}

    {% if candidates and candidates.rows | length > 0 %}

      {% for cand_row in candidates %}

        {% set table_fqn = cand_row['TABLE_FQN'] %}

        {{ log("extract_query_operator_columns: finding queries for " ~ table_fqn, info=true) }}

        {# Step 2: Get representative query_ids — one per distinct parameterized hash #}
        {% set queries_sql %}
          select query_id, query_start_time
          from (
              select
                  ca.query_id,
                  ca.query_start_time,
                  row_number() over (
                      partition by qh.query_parameterized_hash
                      order by ca.query_start_time desc
                  ) as rn
              from {{ col_access_table }} as ca
              inner join {{ query_history_table }} as qh
                  on ca.query_id = qh.query_id
              where ca.table_fqn = '{{ table_fqn }}'
                  and ca.query_start_time >= dateadd(day, -14, current_timestamp())
                  and qh.query_type = 'SELECT'
          )
          where rn = 1
          limit {{ queries_per_table }}
        {% endset %}

        {% set queries_result = run_query(queries_sql) %}

        {% if queries_result and queries_result.rows | length > 0 %}

          {{ log("extract_query_operator_columns: analyzing " ~ (queries_result.rows | length) ~ " queries for " ~ table_fqn, info=true) }}

          {# Step 3: For each query_id, parse operators and merge matches #}
          {% for q_row in queries_result %}

            {% set qid = q_row['QUERY_ID'] %}
            {% set qstart = q_row['QUERY_START_TIME'] %}

            {% set merge_sql %}
              merge into {{ operator_table }} as target
              using (
                  select
                      md5('{{ qid }}' || '|' || '{{ table_fqn }}' || '|' || cols.column_name || '|' || ops.operator_type) as operator_column_key,
                      '{{ qid }}' as query_id,
                      '{{ table_fqn }}' as table_fqn,
                      cols.column_name,
                      ops.operator_type,
                      '{{ qstart }}'::timestamp_ntz as query_start_time,
                      cast('{{ qstart }}'::timestamp_ntz as date) as access_date
                  from (
                      select
                          operator_type,
                          case
                              when operator_type = 'Filter'
                                  then operator_attributes:filter_condition::string
                              when operator_type = 'Join'
                                  then operator_attributes:equality_join_condition::string
                          end as condition_text
                      from table(get_query_operator_stats('{{ qid }}'))
                      where operator_type in ('Filter', 'Join')
                  ) as ops
                  cross join (
                      select upper(column_name) as column_name
                      from {{ table_columns_ref }}
                      where table_fqn = '{{ table_fqn }}'
                  ) as cols
                  where ops.condition_text ilike '%' || cols.column_name || '%'
              ) as source
              on target.operator_column_key = source.operator_column_key
              when not matched then insert
                  (operator_column_key, query_id, table_fqn, column_name, operator_type, query_start_time, access_date)
              values
                  (source.operator_column_key, source.query_id, source.table_fqn, source.column_name, source.operator_type, source.query_start_time, source.access_date)
            {% endset %}

            {% do run_query(merge_sql) %}

          {% endfor %}

          {{ log("extract_query_operator_columns: completed " ~ table_fqn ~ " (" ~ (queries_result.rows | length) ~ " queries)", info=true) }}

        {% else %}
          {{ log("extract_query_operator_columns: no recent queries for " ~ table_fqn ~ ", skipping.", info=true) }}
        {% endif %}

      {% endfor %}

    {% else %}
      {{ log("extract_query_operator_columns: no candidates found for today, skipping.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}
