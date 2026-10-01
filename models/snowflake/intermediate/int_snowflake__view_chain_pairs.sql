{{
  config(
    materialized='table',
  )
}}

{#--
  One row per (upstream view or ephemeral, downstream table) pair where the path from
  the upstream model to the table passes only through views and ephemerals. Every build
  of the table recomputes the upstream model's SQL inline.

  path_length is the fewest hops from the upstream model to the table (1 = feeds it
  directly). Diamond DAGs, where the table is reachable along several paths, keep the
  shortest.
--#}

with view_edges as (
    -- all edges where the parent is a view/ephemeral, flagging whether the child
    -- is a non-view/ephemeral (i.e., a table that terminates the walk)
    select
        model_fqn                                                           as parent_fqn,
        neighbor_fqn                                                        as child_fqn,
        lower(neighbor_materialized) not in ('view', 'ephemeral')          as child_is_table
    from {{ ref('int_snowflake__model_relationships') }}
    where relationship = 'child'
      and lower(model_materialized) in ('view', 'ephemeral')
),

view_reachability (root_fqn, current_fqn, hops, is_terminal) as (
    -- anchor: each view's direct children, recording the source view as root
    select
        parent_fqn  as root_fqn,
        child_fqn   as current_fqn,
        1           as hops,
        child_is_table
    from view_edges

    union all

    -- recursive: continue walking through view children only; stop at tables
    select
        vr.root_fqn,
        ve.child_fqn,
        vr.hops + 1,
        ve.child_is_table
    from view_reachability  as vr
    join view_edges         as ve on ve.parent_fqn = vr.current_fqn
    where not vr.is_terminal
),

pairs as (
    select
        root_fqn    as upstream_fqn,
        current_fqn as table_fqn,
        min(hops)   as path_length
    from view_reachability
    where is_terminal
    group by root_fqn, current_fqn
)

select
    p.upstream_fqn,
    lower(up.materialized)  as upstream_materialized,
    up.dbt_model            as upstream_dbt_model,
    up.model_name           as upstream_model_name,
    up.package_name         as upstream_package_name,
    p.table_fqn,
    t.dbt_model             as table_dbt_model,
    t.model_name            as table_model_name,
    p.path_length
from pairs as p
inner join {{ ref('int_dbt__relations') }} as up on up.table_fqn = p.upstream_fqn
left join {{ ref('int_dbt__relations') }} as t on t.table_fqn = p.table_fqn
