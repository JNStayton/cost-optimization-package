{{
  config(
    materialized='table'
  )
}}

{% set all_model_nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list %}

{# Filter out disabled models #}
{% set model_nodes = [] %}
{% for node in all_model_nodes %}
    {% if node.config and node.config.enabled != false %}
        {% do model_nodes.append(node) %}
    {% endif %}
{% endfor %}

{# Build lookup: unique_id -> node, for model nodes only #}
{% set model_node_map = {} %}
{% for node in model_nodes %}
    {% do model_node_map.update({node.unique_id: node}) %}
{% endfor %}

{# Build reverse lookup: unique_id -> list of child unique_ids (models only) #}
{% set node_children = {} %}
{% for node in model_nodes %}
    {% for parent_id in node.depends_on.nodes %}
        {% if parent_id in model_node_map %}
            {% if parent_id not in node_children %}
                {% do node_children.update({parent_id: []}) %}
            {% endif %}
            {% do node_children[parent_id].append(node.unique_id) %}
        {% endif %}
    {% endfor %}
{% endfor %}

with dbt_relations as (
    {% if model_nodes | length > 0 %}
        {% for node in model_nodes %}
            {% set parent_fqns = [] %}
            {% for parent_id in node.depends_on.nodes %}
                {% if parent_id in model_node_map %}
                    {% set p = model_node_map[parent_id] %}
                    {% do parent_fqns.append(
                        ((p.database or '') ~ '.' ~ (p.schema or '') ~ '.' ~ (p.alias if p.alias else p.name)) | upper
                    ) %}
                {% endif %}
            {% endfor %}

            {% set child_fqns = [] %}
            {% for child_id in node_children.get(node.unique_id, []) %}
                {% set c = model_node_map[child_id] %}
                {% do child_fqns.append(
                    ((c.database or '') ~ '.' ~ (c.schema or '') ~ '.' ~ (c.alias if c.alias else c.name)) | upper
                ) %}
            {% endfor %}

            select
                '{{ target.type }}' as platform,
                upper('{{ (node.database or "") }}') as database_name,
                upper('{{ (node.schema or "") }}') as schema_name,
                upper('{{ (node.alias if node.alias else node.name) }}') as table_name,
                upper('{{ (node.database or "") }}') || '.' || upper('{{ (node.schema or "") }}') || '.' || upper('{{ (node.alias if node.alias else node.name) }}') as table_fqn,
                '{{ node.unique_id }}' as dbt_model,
                '{{ node.name }}' as model_name,
                '{{ node.alias if node.alias else node.name }}' as alias,
                '{{ node.package_name }}' as package_name,
                '{{ node.config.materialized if node.config and node.config.materialized else "" }}' as materialized,
                '{{ (node.database or "") }}.{{ (node.schema or "") }}.{{ (node.alias if node.alias else node.name) }}' as relation_fqn,
                array_construct({% for fqn in parent_fqns %}'{{ fqn }}'{% if not loop.last %}, {% endif %}{% endfor %}) as parent_models,
                array_construct({% for fqn in child_fqns %}'{{ fqn }}'{% if not loop.last %}, {% endif %}{% endfor %}) as child_models
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        select
            cast(null as string) as platform,
            cast(null as string) as database_name,
            cast(null as string) as schema_name,
            cast(null as string) as table_name,
            cast(null as string) as table_fqn,
            cast(null as string) as dbt_model,
            cast(null as string) as model_name,
            cast(null as string) as alias,
            cast(null as string) as package_name,
            cast(null as string) as materialized,
            cast(null as string) as relation_fqn,
            array_construct() as parent_models,
            array_construct() as child_models
        where 1 = 0
    {% endif %}
)

select * from dbt_relations
