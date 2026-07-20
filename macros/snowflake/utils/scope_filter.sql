{#--
  Generates a SQL WHERE clause fragment for project scope filtering.
  Used by all gold-layer views to apply consistent scope logic.

  Behavior:
    - dbt_monitored_projects = [] (default): filters to [project_name]
    - dbt_monitored_projects = ['*']: no project filter (all dbt projects pass)
    - dbt_monitored_projects = ['a', 'b']: filters to listed projects
    - include_full_platform_insights = true: allows rows with no project association (account-wide signals)
    - include_full_platform_insights = false (default): only project-tied rows pass

  Arguments:
    - project_col: the column name containing the project name (default: 'node_project_name')
    - allow_null_col: the column to check for null (account-wide recs). Default: 'node_id'

  Usage in gold views:
    WHERE ar.backlog_status = 'actionable'
      AND ({{ scope_filter('ar.node_project_name', 'ar.node_id') }})
--#}

{% macro scope_filter(project_col='node_project_name', allow_null_col='node_id') %}
{%- set monitored_projects = var('dbt_monitored_projects', []) -%}
{%- if monitored_projects | length == 0 -%}
  {%- set monitored_projects = [project_name] -%}
{%- endif -%}
{%- set monitor_all = (monitored_projects | length == 1 and monitored_projects[0] == '*') -%}
{%- set platform_insights = var('include_full_platform_insights', false) -%}

{%- if monitor_all and platform_insights -%}
    true
{%- elif monitor_all -%}
    ({{ project_col }} is not null)
{%- elif platform_insights -%}
    ({{ project_col }} in (
        {%- for proj in monitored_projects -%}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ) or {{ allow_null_col }} is null)
{%- else -%}
    ({{ project_col }} in (
        {%- for proj in monitored_projects -%}
            '{{ proj }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    ))
{%- endif -%}
{% endmacro %}
