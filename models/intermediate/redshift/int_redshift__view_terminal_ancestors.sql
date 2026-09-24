{{
  config(
    materialized='table'
  )
}}

{#
  For every view/ephemeral model, recursively walks int_redshift__model_edges
  (model -> model, upward) and int_redshift__model_source_edges (model ->
  source) to resolve the FULL set of terminal ancestors: source() tables, or
  table/incremental dbt models.

  Resolution is based entirely on the dbt manifest graph (ref()/source()
  calls, resolved at compile time), not Redshift's own pg_depend/pg_rewrite
  catalogs. Those catalogs never populate for late-binding views (bind=false,
  a documented dbt-redshift production pattern) or for any view chain that
  touches an external/shared-storage table — the manifest graph has no such
  gap, since it's unaffected by how a view happens to be bound in the
  warehouse.

  Materialized as a table, not a view: this model's whole purpose is to be
  joined against by int_redshift__query_view_access on a per-terminal-table
  basis, and Redshift's WITH RECURSIVE views become unreliable once a
  downstream consumer does more than a simple column read against them.

  Depth-capped at 20, matching int_redshift__view_chains' convention, as a
  guard against cycles.
#}

with recursive ancestor_walk (
    origin_database, origin_schema, origin_name,
    current_database, current_schema, current_name, current_materialization,
    depth
) as (

    -- Base case: the view/ephemeral model itself
    select
        lower(database_name), lower(schema_name), lower(table_name),
        lower(database_name), lower(schema_name), lower(table_name), lower(materialized),
        0 as depth

    from {{ ref('int_dbt__relations') }}
    where lower(materialized) in ('view', 'ephemeral')

    union all

    -- Recursive step: walk up through model_edges as long as the current node
    -- is itself a view/ephemeral (a table/incremental ancestor is terminal —
    -- don't walk past it).
    select
        aw.origin_database, aw.origin_schema, aw.origin_name,
        e.parent_database, e.parent_schema, e.parent_name, e.parent_materialization,
        aw.depth + 1

    from ancestor_walk aw
    inner join {{ ref('int_redshift__model_edges') }} e
        on aw.current_database = e.child_database
        and aw.current_schema  = e.child_schema
        and aw.current_name    = e.child_name
    where aw.current_materialization in ('view', 'ephemeral')
        and aw.depth < 20

),

model_terminals as (

    -- Terminal ancestors that are dbt-managed table/incremental models
    select distinct
        origin_database, origin_schema, origin_name,
        current_database as terminal_database,
        current_schema   as terminal_schema,
        current_name     as terminal_name,
        'model'          as terminal_kind

    from ancestor_walk
    where current_materialization in ('table', 'incremental')
        and depth > 0

),

source_terminals as (

    -- At every view/ephemeral hop along the walk (including the origin
    -- itself, depth 0), check for a direct source() dependency.
    select distinct
        aw.origin_database, aw.origin_schema, aw.origin_name,
        mse.source_database   as terminal_database,
        mse.source_schema     as terminal_schema,
        mse.source_identifier as terminal_name,
        'source'               as terminal_kind

    from ancestor_walk aw
    inner join {{ ref('int_redshift__model_source_edges') }} mse
        on aw.current_database = mse.model_database
        and aw.current_schema  = mse.model_schema
        and aw.current_name    = mse.model_name
    where aw.current_materialization in ('view', 'ephemeral')

)

select
    origin_database as database_name,
    origin_schema   as view_schema,
    origin_name     as view_name,
    terminal_database,
    terminal_schema,
    terminal_name,
    terminal_database || '.' || terminal_schema || '.' || terminal_name as terminal_fqn,
    terminal_kind
from model_terminals

union all

select
    origin_database as database_name,
    origin_schema   as view_schema,
    origin_name     as view_name,
    terminal_database,
    terminal_schema,
    terminal_name,
    terminal_database || '.' || terminal_schema || '.' || terminal_name as terminal_fqn,
    terminal_kind
from source_terminals
