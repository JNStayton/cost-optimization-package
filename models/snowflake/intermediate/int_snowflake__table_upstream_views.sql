{{
  config(
    materialized='table',
  )
}}

{#--
  One row per table at the end of a view chain: the views and ephemerals its builds
  recompute inline, nearest first, and the view F2 recommends materializing for it.
  Built from int_snowflake__view_chain_pairs and
  fct_snowflake__table_materialization_candidates.

    upstream_view_chain     e.g. 'int_order_item_summary (ephemeral), int_customer_order_items_geo (view), ...'
    upstream_view_count     views and ephemerals in the chain
    chain_recommended_view  the candidate view with the highest net savings for this
                            table (the same choice as the materialization fact: ties to
                            the view nearest the table); null when no view in the chain
                            is a candidate (e.g. only ephemerals)
--#}

with chain as (
    select
        table_fqn,
        table_dbt_model,
        table_model_name,
        listagg(upstream_model_name || ' (' || upstream_materialized || ')', ', ')
            within group (order by path_length, upstream_model_name) as upstream_view_chain,
        count(distinct upstream_fqn) as upstream_view_count
    from {{ ref('int_snowflake__view_chain_pairs') }}
    group by table_fqn, table_dbt_model, table_model_name
),

recommended as (
    select
        p.table_fqn,
        p.upstream_fqn          as chain_recommended_view_fqn,
        p.upstream_model_name   as chain_recommended_view
    from {{ ref('int_snowflake__view_chain_pairs') }} as p
    inner join {{ ref('fct_snowflake__table_materialization_candidates') }} as tm
        on tm.table_fqn = p.upstream_fqn
       and tm.chain_role = 'recommended'
    qualify row_number() over (
        partition by p.table_fqn
        order by tm.net_recompute_s_saved desc, p.path_length, p.upstream_fqn
    ) = 1
)

select
    c.table_fqn,
    c.table_dbt_model,
    c.table_model_name,
    c.upstream_view_chain,
    c.upstream_view_count,
    r.chain_recommended_view,
    r.chain_recommended_view_fqn
from chain as c
left join recommended as r on r.table_fqn = c.table_fqn
