{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

with view_like_edges as (
    -- edges between view/ephemeral nodes, treating ephemerals as transparent
    select
        model_fqn    as parent_fqn,
        neighbor_fqn as child_fqn
    from {{ ref('int_snowflake__model_relationships') }}
    where relationship = 'child'
      and lower(model_materialized)    in ('view', 'ephemeral')
      and lower(neighbor_materialized) in ('view', 'ephemeral')
),

chain_roots as (
    -- view/ephemeral nodes that start a chain: have at least one view/ephemeral
    -- child but no view/ephemeral parent
    select table_fqn as model_fqn
    from {{ ref('int_dbt__relations') }}
    where lower(materialized) in ('view', 'ephemeral')
      and not exists (select 1 from view_like_edges where child_fqn  = table_fqn)
      and     exists (select 1 from view_like_edges where parent_fqn = table_fqn)
),

view_chains (model_fqn, chain_id, depth) as (
    -- anchor: roots seed the chain with themselves as chain_id at depth 0
    select
        model_fqn,
        model_fqn as chain_id,
        0         as depth
    from chain_roots

    union all

    -- recursive: walk one hop downstream, inheriting the chain_id from the root
    select
        e.child_fqn,
        vc.chain_id,
        vc.depth + 1
    from view_chains     as vc
    join view_like_edges as e on e.parent_fqn = vc.model_fqn
),

chain_stats as (
    select
        model_fqn,
        chain_id,
        depth,
        count(*) over (partition by chain_id) as chain_size
    from view_chains
),

downstream_tables as (
    -- all non-view/ephemeral dbt models that are direct children of any node in the chain
    select
        cs.chain_id,
        mr.neighbor_fqn as downstream_table_fqn
    from chain_stats as cs
    join {{ ref('int_snowflake__model_relationships') }} as mr
        on mr.model_fqn = cs.model_fqn
       and mr.relationship = 'child'
       and lower(mr.neighbor_materialized) not in ('view', 'ephemeral')
),

downstream_table_counts as (
    select
        chain_id,
        count(distinct downstream_table_fqn) as downstream_table_count
    from downstream_tables
    group by chain_id
),

downstream_table_fqns_agg as (
    select
        chain_id,
        array_agg(distinct downstream_table_fqn) as downstream_table_fqns
    from downstream_tables
    group by chain_id
)

select
    cs.model_fqn,
    cs.chain_id,
    cs.depth,
    cs.chain_size,
    coalesce(dtc.downstream_table_count, 0)              as downstream_table_count,
    coalesce(dtfa.downstream_table_fqns, array_construct()) as downstream_table_fqns
from chain_stats as cs
left join downstream_table_counts as dtc
    on dtc.chain_id = cs.chain_id
left join downstream_table_fqns_agg as dtfa
    on dtfa.chain_id = cs.chain_id
