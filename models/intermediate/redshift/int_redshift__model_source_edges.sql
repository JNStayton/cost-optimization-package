{{
    config(
        materialized='table'
    )
}}

{#
  Redshift-only companion to int_dbt__relations: one row per (model, source)
  edge. int_dbt__relations only walks graph.nodes (dbt models), so any
  depends_on.nodes entry pointing at a source() call is not captured there —
  every staging model whose only dependency is a source() would otherwise
  have an empty parent_models array. Kept as a separate, Redshift-scoped
  model rather than modifying int_dbt__relations directly, since that model
  is shared across every platform this package supports.

  source_fqn is resolved from graph.sources (database/schema/identifier) —
  the same physical identity Redshift's sys_query_detail.table_name uses for
  a scanned table, letting downstream models join query telemetry directly
  to a source without depending on pg_depend or query_text matching.
#}

{% set all_model_nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list %}

{% set model_nodes = [] %}
{% for node in all_model_nodes %}
    {% if node.config and node.config.enabled != false %}
        {% do model_nodes.append(node) %}
    {% endif %}
{% endfor %}

{% set source_node_map = {} %}
{% for src in graph.sources.values() %}
    {% do source_node_map.update({src.unique_id: src}) %}
{% endfor %}

{% set edge_selects = [] %}
{% for node in model_nodes %}
    {% for parent_id in node.depends_on.nodes %}
        {% if parent_id in source_node_map %}
            {% set s = source_node_map[parent_id] %}
            {% set select_sql %}
                select
                    '{{ node.database or "" }}'   as model_database,
                    '{{ node.schema or "" }}'     as model_schema,
                    '{{ node.alias if node.alias else node.name }}' as model_name,
                    '{{ node.unique_id }}'        as dbt_model,
                    '{{ s.database or "" }}'      as source_database,
                    '{{ s.schema or "" }}'        as source_schema,
                    '{{ s.identifier }}'          as source_identifier
            {% endset %}
            {% do edge_selects.append(select_sql) %}
        {% endif %}
    {% endfor %}
{% endfor %}

with model_source_edges as (
    {% if edge_selects | length > 0 %}
        {{ edge_selects | join('\n    union all\n') }}
    {% else %}
        select
            cast(null as varchar) as model_database,
            cast(null as varchar) as model_schema,
            cast(null as varchar) as model_name,
            cast(null as varchar) as dbt_model,
            cast(null as varchar) as source_database,
            cast(null as varchar) as source_schema,
            cast(null as varchar) as source_identifier
        where 1 = 0
    {% endif %}
)

select
    lower(model_database)                                                   as model_database,
    lower(model_schema)                                                     as model_schema,
    lower(model_name)                                                       as model_name,
    dbt_model,
    lower(source_database)                                                  as source_database,
    lower(source_schema)                                                    as source_schema,
    lower(source_identifier)                                                as source_identifier,
    lower(source_database) || '.' || lower(source_schema) || '.' || lower(source_identifier) as source_fqn
from model_source_edges
