{#--
  Returns a SQL boolean expression: true when a relation is excluded from
  recommendations by the dbt_excluded_schemas / dbt_excluded_targets vars.

  - dbt_excluded_schemas: schema name patterns (LIKE, case-insensitive), e.g. ['DBT_%'].
  - dbt_excluded_targets: target names, e.g. ['dev']. A relation is excluded only when
    every target it was built under is excluded (a table built under 'dev' and 'prod'
    stays). Relations with no known target are never excluded by target.

  Both default to [], which excludes nothing.

  Usage:
    {{ relation_is_excluded('schema_name', 'target_names') }}
    {{ relation_is_excluded("split_part(table_fqn, '.', 2)", 'null') }}   -- schema check only
--#}

{% macro relation_is_excluded(schema_expr, target_names_expr) %}
  {%- set excluded_schemas = var('dbt_excluded_schemas', []) -%}
  {%- set excluded_targets = var('dbt_excluded_targets', []) -%}
  (
    {%- if excluded_schemas | length > 0 %}
    coalesce(upper({{ schema_expr }}) like any (
        {%- for pattern in excluded_schemas -%}
        '{{ pattern | upper }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ), false)
    {%- else %}
    false
    {%- endif %}
    {%- if excluded_targets | length > 0 and target_names_expr != 'null' %}
    or (
        coalesce(array_size({{ target_names_expr }}), 0) > 0
        and array_size(array_except({{ target_names_expr }}, array_construct(
            {%- for t in excluded_targets -%}
            '{{ t }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        ))) = 0
    )
    {%- endif %}
  )
{% endmacro %}
