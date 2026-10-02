{{
  config(
    materialized='view',
  )
}}

{#--
  Suggested clustering keys per table, from the latest fct_snowflake__clustering_key_candidates
  snapshot. One definition for every gold view, so the backlog, top recommendations and the
  dbt model view give the same key and cluster_by config.

  A column is recommended when its filter evidence is at least 70% of the top column's;
  the rest are additional candidates. Columns keep recommended_key_position order.
--#}

with candidates as (
    select distinct
        table_fqn,
        column_name,
        recommended_key_position,
        filter_query_count::float
            / nullif(max(filter_query_count) over (partition by table_fqn), 0) >= 0.70 as is_recommended
    from {{ ref('fct_snowflake__clustering_key_candidates') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('fct_snowflake__clustering_key_candidates') }})
),

summarized as (
    select
        table_fqn,
        listagg(case when is_recommended then column_name end, ', ')
            within group (order by recommended_key_position) as suggested_clustering_key,
        listagg(case when not is_recommended then column_name end, ', ')
            within group (order by recommended_key_position) as additional_clustering_candidates
    from candidates
    group by table_fqn
)

select
    table_fqn,
    nullif(suggested_clustering_key, '')          as suggested_clustering_key,
    nullif(additional_clustering_candidates, '')  as additional_clustering_candidates,
    case
        when nullif(suggested_clustering_key, '') is not null
            then '{% raw %}{{ config(cluster_by=[{% endraw %}'''
                || replace(suggested_clustering_key, ', ', ''', ''')
                || '''{% raw %}]) }}{% endraw %}'
    end as cluster_by_config
from summarized
