{{
  config(
    materialized='table'
  )
}}

{% set model_nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list %}

with dbt_relations as (
    {% if model_nodes | length > 0 %}
        {% for node in model_nodes %}
            select
                '{{ target.type }}' as platform,
                upper('{{ (node.database or "") }}') as database_name,
                upper('{{ (node.schema or "") }}') as schema_name,
                upper('{{ (node.alias if node.alias else node.name) }}') as table_name,
                '{{ node.unique_id }}' as dbt_model,
                '{{ node.name }}' as model_name,
                '{{ node.alias if node.alias else node.name }}' as alias,
                '{{ node.package_name }}' as package_name,
                '{{ node.config.materialized if node.config and node.config.materialized else "" }}' as materialized,
                '{{ (node.database or "") }}.{{ (node.schema or "") }}.{{ (node.alias if node.alias else node.name) }}' as relation_fqn
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        select
            cast(null as string) as platform,
            cast(null as string) as database_name,
            cast(null as string) as schema_name,
            cast(null as string) as table_name,
            cast(null as string) as dbt_model,
            cast(null as string) as model_name,
            cast(null as string) as alias,
            cast(null as string) as package_name,
            cast(null as string) as materialized,
            cast(null as string) as relation_fqn
        where 1 = 0
    {% endif %}
)

select * from dbt_relations
