{{
  config(
    materialized='table',
    enabled=(target.type == 'snowflake')
  )
}}

with flattened_parents as (
    select distinct
        r.table_fqn        as model_fqn,
        r.materialized     as model_materialized,
        p.value::string    as neighbor_fqn,
        'parent'           as relationship
    from {{ ref('int_dbt__relations') }} as r,
    lateral flatten(input => r.parent_models) as p
),

flattened_children as (
    select distinct
        r.table_fqn        as model_fqn,
        r.materialized     as model_materialized,
        c.value::string    as neighbor_fqn,
        'child'            as relationship
    from {{ ref('int_dbt__relations') }} as r,
    lateral flatten(input => r.child_models) as c
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
