{#
  Transitive downstream model counts for view nodes, computed via recursive SQL.

  IMPORTANT: This model uses WITH RECURSIVE and must only reference non-ephemeral models.
  If an ephemeral ref() is ever added here, dbt will inject non-recursive CTEs before this
  query, stripping the RECURSIVE keyword and silently breaking the transitive closure.
  See int_redshift__model_edges for context.

  Materialized as a table (configured in the model's schema yml). A downstream
  consumer that unnests downstream_table_fqns (a SUPER array) needs this to be a
  physical table, not a view — Redshift has to inline a view's WITH RECURSIVE
  definition at query time for any consumer that does more than a simple
  column read, and that combination is unreliable.
#}

with recursive view_descendants (parent_database, parent_schema, view_name, child_database, child_schema, child_name, child_materialization, depth) as (

    -- Base case: direct children of view nodes
    select
        e.parent_database,
        e.parent_schema,
        e.parent_name                           as view_name,
        e.child_database,
        e.child_schema,
        e.child_name,
        e.child_materialization,
        1                                       as depth

    from {{ ref('int_redshift__model_edges') }} e
    where e.parent_materialization = 'view'

    union all

    -- Recursive step: follow children of already-visited descendants
    select
        vd.parent_database,
        vd.parent_schema,
        vd.view_name,
        e.child_database,
        e.child_schema,
        e.child_name,
        e.child_materialization,
        vd.depth + 1

    from view_descendants vd
    inner join {{ ref('int_redshift__model_edges') }} e
        on vd.child_database  = e.parent_database
        and vd.child_schema   = e.parent_schema
        and vd.child_name     = e.parent_name
    where vd.depth < 20   -- safety cap against cycles

),

aggregated as (

    select
        parent_schema                                                                           as view_schema,
        view_name,
        count(distinct child_schema || '.' || child_name)                                      as total_downstream_model_count,
        count(distinct case
            when child_materialization in ('table', 'incremental')
            then child_schema || '.' || child_name
        end)                                                                                   as downstream_table_count,
        min(case
            when child_materialization in ('table', 'incremental')
            then depth
        end)                                                                                   as min_hops_to_table,
        max(depth)                                                                             as max_chain_depth

    from view_descendants
    group by 1, 2

),

table_fqns_deduped as (

    select distinct
        parent_schema                                               as view_schema,
        view_name,
        child_database || '.' || child_schema || '.' || child_name as downstream_table_fqn

    from view_descendants
    where child_materialization in ('table', 'incremental')

),

table_fqn_agg as (

    select
        view_schema,
        view_name,
        json_parse(
            '[' ||
            listagg('"' || downstream_table_fqn || '"', ',')
                within group (order by downstream_table_fqn)
            || ']'
        )                                                          as downstream_table_fqns

    from table_fqns_deduped
    group by 1, 2

)

select
    a.view_schema,
    a.view_name,
    a.total_downstream_model_count,
    a.downstream_table_count,
    a.min_hops_to_table,
    a.max_chain_depth,
    t.downstream_table_fqns

from aggregated a
left join table_fqn_agg t
    on a.view_schema = t.view_schema
    and a.view_name  = t.view_name
