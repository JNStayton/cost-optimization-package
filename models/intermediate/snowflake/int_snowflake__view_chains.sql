{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

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

reachable_tables as (
    -- one row per (view, downstream_table) pair; min hops handles diamond DAGs
    -- where the same table is reachable via multiple paths
    select
        root_fqn    as model_fqn,
        current_fqn as table_fqn,
        min(hops)   as min_path_length
    from view_reachability
    where is_terminal
    group by root_fqn, current_fqn
),

min_depth as (
    select
        model_fqn,
        min(min_path_length) as min_hops_to_table
    from reachable_tables
    group by model_fqn
),

downstream_agg as (
    select
        model_fqn,
        count(distinct table_fqn)       as downstream_table_count,
        array_agg(distinct table_fqn)   as downstream_table_fqns
    from reachable_tables
    group by model_fqn
)

select
    da.model_fqn,
    da.downstream_table_count,
    da.downstream_table_fqns,
    md.min_hops_to_table
from downstream_agg as da
join min_depth      as md on md.model_fqn = da.model_fqn
