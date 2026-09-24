{{
  config(
    materialized='table'
  )
}}

with flattened_parents as (
    select distinct
        r.table_fqn       as model_fqn,
        r.materialized    as model_materialized,
        p.neighbor_fqn,
        'parent'          as relationship
    from {{ ref('int_dbt__relations') }} as r
    lateral view explode(r.parent_models) p as neighbor_fqn
),

flattened_children as (
    select distinct
        r.table_fqn       as model_fqn,
        r.materialized    as model_materialized,
        c.neighbor_fqn,
        'child'           as relationship
    from {{ ref('int_dbt__relations') }} as r
    lateral view explode(r.child_models) c as neighbor_fqn
),

all_relationships as (
    select * from flattened_parents
    union all
    select * from flattened_children
)

select
    ar.model_fqn,
    ar.model_materialized,
    ar.neighbor_fqn,
    ar.relationship,
    neighbor.materialized as neighbor_materialized
from all_relationships as ar
left join {{ ref('int_dbt__relations') }} as neighbor
    on neighbor.table_fqn = ar.neighbor_fqn
