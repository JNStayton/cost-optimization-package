{{ config(materialized='view') }}

{% if var('use_mock_data', false) %}

select
    catalog_name,
    schema_name,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable
from {{ ref('databricks_columns') }}

{% else %}

{#--
  Scope the system.information_schema.columns scan. Unfiltered it returns every
  column of every table in the catalog (264k+ rows on real catalogs), so every
  downstream mart that joins column metadata pays a multi-minute scan.

  By default we auto-derive the catalogs and schemas this dbt project actually
  uses (models, snapshots, seeds, and sources) from the graph and restrict the
  scan to those — fast out of the box, no configuration required. Override with:
    - columns_scan_schemas : explicit list of schemas to scan
    - columns_scan_catalogs: explicit list of catalogs to scan
  Set either to an empty list to disable that filter (e.g. to scan everything).
--#}
{% set derived_catalogs = [] %}
{% set derived_schemas = [] %}
{% if execute %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ['model', 'snapshot', 'seed'] %}
            {% if node.database and node.database not in derived_catalogs %}{% do derived_catalogs.append(node.database) %}{% endif %}
            {% if node.schema and node.schema not in derived_schemas %}{% do derived_schemas.append(node.schema) %}{% endif %}
        {% endif %}
    {% endfor %}
    {% for src in graph.sources.values() %}
        {% if src.database and src.database not in derived_catalogs %}{% do derived_catalogs.append(src.database) %}{% endif %}
        {% if src.schema and src.schema not in derived_schemas %}{% do derived_schemas.append(src.schema) %}{% endif %}
    {% endfor %}
{% endif %}

{% set scan_catalogs = var('columns_scan_catalogs', derived_catalogs) %}
{% set scan_schemas  = var('columns_scan_schemas', derived_schemas) %}

select
    table_catalog as catalog_name,
    table_schema  as schema_name,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable
from {{ source('databricks_information_schema', 'columns') }}
where table_schema != 'information_schema'
    and table_catalog != 'system'
    {% if scan_catalogs | length > 0 %}
    and table_catalog in ({% for c in scan_catalogs %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {% endif %}
    {% if scan_schemas | length > 0 %}
    and table_schema in ({% for s in scan_schemas %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {% endif %}

{% endif %}
