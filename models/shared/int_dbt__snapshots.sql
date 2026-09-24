{{
  config(
    materialized='table'
  )
}}

{#--
  Vendor-neutral graph metadata for dbt snapshot nodes.

  Mirrors the shape of int_dbt__relations but for snapshots specifically.
  Exposes per-snapshot config attributes — strategy, updated_at, check_cols,
  unique_key, invalidate_hard_deletes — plus parent and child FQN arrays so
  downstream models can join to source table activity and downstream model
  dependencies.

  Used by fct_databricks__snapshot_optimization_candidates to evaluate run
  efficiency and strategy fit. May also serve other adapters when they need
  graph context for snapshots.
--#}

{% set all_snapshot_nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "snapshot") | list %}

{% set snapshot_nodes = [] %}
{% for node in all_snapshot_nodes %}
    {% if node.config and node.config.enabled != false %}
        {% do snapshot_nodes.append(node) %}
    {% endif %}
{% endfor %}

{# Find downstream consumers (models or other snapshots that ref this snapshot) #}
{% set node_children = {} %}
{% set all_consumer_nodes = graph.nodes.values() | selectattr("resource_type", "in", ["model", "snapshot"]) | list %}
{% for node in all_consumer_nodes %}
    {% for parent_id in node.depends_on.nodes %}
        {% if parent_id.startswith('snapshot.') %}
            {% if parent_id not in node_children %}
                {% do node_children.update({parent_id: []}) %}
            {% endif %}
            {% do node_children[parent_id].append(node.unique_id) %}
        {% endif %}
    {% endfor %}
{% endfor %}

with dbt_snapshots as (
    {% if snapshot_nodes | length > 0 %}
        {% for node in snapshot_nodes %}
            {# Parent FQNs — sources, models, or seeds #}
            {% set parent_fqns = [] %}
            {% for parent_id in node.depends_on.nodes %}
                {% set parent_node = graph.nodes.get(parent_id) or graph.sources.get(parent_id) %}
                {% if parent_node %}
                    {% if parent_id.startswith('source.') %}
                        {% set parent_table = parent_node.identifier if parent_node.identifier else parent_node.name %}
                        {% do parent_fqns.append(
                            ((parent_node.database or '') ~ '.' ~ (parent_node.schema or '') ~ '.' ~ parent_table) | upper
                        ) %}
                    {% else %}
                        {% do parent_fqns.append(
                            ((parent_node.database or '') ~ '.' ~ (parent_node.schema or '') ~ '.' ~ (parent_node.alias if parent_node.alias else parent_node.name)) | upper
                        ) %}
                    {% endif %}
                {% endif %}
            {% endfor %}

            {# Child FQNs — downstream models or snapshots #}
            {% set child_fqns = [] %}
            {% for child_id in node_children.get(node.unique_id, []) %}
                {% set c = graph.nodes.get(child_id) %}
                {% if c %}
                    {% do child_fqns.append(
                        ((c.database or '') ~ '.' ~ (c.schema or '') ~ '.' ~ (c.alias if c.alias else c.name)) | upper
                    ) %}
                {% endif %}
            {% endfor %}

            {# check_cols may be a list, or the string 'all' as a sentinel.
               Normalize both into a list so the column is always an array. #}
            {% set check_cols_list = [] %}
            {% if node.config.check_cols is string %}
                {% do check_cols_list.append(node.config.check_cols) %}
            {% elif node.config.check_cols %}
                {% set check_cols_list = node.config.check_cols %}
            {% endif %}

            {# unique_key may be a string or a list (composite). Cast to string. #}
            {% set unique_key_str = node.config.unique_key %}
            {% if unique_key_str is not string and unique_key_str %}
                {% set unique_key_str = unique_key_str | join(',') %}
            {% endif %}

            {% set ihd = node.config.invalidate_hard_deletes | default(false) %}

            select
                '{{ target.type }}' as platform,
                upper('{{ node.database or "" }}') as database_name,
                upper('{{ node.schema or "" }}') as schema_name,
                upper('{{ node.alias if node.alias else node.name }}') as table_name,
                upper('{{ node.database or "" }}') || '.' || upper('{{ node.schema or "" }}') || '.' || upper('{{ node.alias if node.alias else node.name }}') as table_fqn,
                '{{ node.unique_id }}' as dbt_snapshot,
                '{{ node.name }}' as snapshot_name,
                '{{ node.alias if node.alias else node.name }}' as alias,
                '{{ node.package_name }}' as package_name,
                '{{ node.config.strategy or "" }}' as strategy,
                '{{ node.config.updated_at or "" }}' as updated_at,
                {{ make_string_array(check_cols_list) }} as check_cols,
                '{{ unique_key_str or "" }}' as unique_key,
                {{ ihd | tojson }} as invalidate_hard_deletes,
                {{ make_string_array(parent_fqns) }} as parent_models,
                {{ make_string_array(child_fqns) }} as child_models,
                {{ node_children.get(node.unique_id, []) | length }} as downstream_model_count
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        select
            cast(null as string) as platform,
            cast(null as string) as database_name,
            cast(null as string) as schema_name,
            cast(null as string) as table_name,
            cast(null as string) as table_fqn,
            cast(null as string) as dbt_snapshot,
            cast(null as string) as snapshot_name,
            cast(null as string) as alias,
            cast(null as string) as package_name,
            cast(null as string) as strategy,
            cast(null as string) as updated_at,
            {{ make_string_array([]) }} as check_cols,
            cast(null as string) as unique_key,
            false as invalidate_hard_deletes,
            {{ make_string_array([]) }} as parent_models,
            {{ make_string_array([]) }} as child_models,
            0 as downstream_model_count
        where 1 = 0
    {% endif %}
)

select * from dbt_snapshots
