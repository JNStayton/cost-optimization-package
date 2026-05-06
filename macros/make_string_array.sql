{% macro make_string_array(values) %}
    {% if target.type == 'snowflake' %}
        array_construct({% for v in values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {% elif target.type == 'databricks' %}
        array({% for v in values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {% elif target.type == 'bigquery' %}
        {% if values | length == 0 %}
            CAST([] AS ARRAY<STRING>)
        {% else %}
            [{% for v in values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %}]
        {% endif %}
    {% elif target.type == 'redshift' %}
        JSON_PARSE('[{% for v in values %}"{{ v }}"{% if not loop.last %},{% endif %}{% endfor %}]')
    {% else %}
        array({% for v in values %}'{{ v }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {% endif %}
{% endmacro %}
