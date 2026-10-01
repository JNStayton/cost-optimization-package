{{ config(alias='access_history') }}
{#- Stand-in for ACCOUNT_USAGE.ACCESS_HISTORY (Enterprise edition), derived from the
    query_history fixture so every query's objects match its text exactly:
      - Reads: the object after FROM (db.schema.name) as a direct object, and as a base
        object when it's a table. Demo views select constants, so they have no base
        tables. DEMO_EVENTS reads also list the columns they use (feeds column access).
      - Writes: CTAS and INSERT targets as modified objects.
    Queries that touch no db.schema.name object (e.g. the warehouse slice's "select 1")
    have no row. The first branch takes its types from the real view. -#}
select query_id, query_start_time, user_name, direct_objects_accessed, base_objects_accessed, objects_modified
from snowflake.account_usage.access_history where false

union all

select
    query_id,
    start_time as query_start_time,
    user_name,
    iff(read_fqn is null, array_construct(), array_construct(object_construct(
        'objectDomain', iff(endswith(read_fqn, '_VIEW'), 'View', 'Table'),
        'objectName', read_fqn,
        'columns', read_columns))) as direct_objects_accessed,
    iff(read_fqn is null or endswith(read_fqn, '_VIEW'), array_construct(), array_construct(object_construct(
        'objectDomain', 'Table',
        'objectName', read_fqn,
        'columns', read_columns))) as base_objects_accessed,
    iff(write_fqn is null, array_construct(), array_construct(object_construct(
        'objectDomain', 'Table',
        'objectName', write_fqn,
        'columns', array_construct()))) as objects_modified
from (
    select
        *,
        case
            when endswith(read_fqn, '.DEMO_EVENTS')
                then array_construct(object_construct('columnName', 'REGION'),
                                     object_construct('columnName', 'EVENT_DATE'),
                                     object_construct('columnName', 'EVENT_ID'),
                                     object_construct('columnName', 'AMOUNT'))
            when endswith(read_fqn, '.DAILY_DEMO_EVENTS')
                then array_construct(object_construct('columnName', 'EVENT_DATE'),
                                     object_construct('columnName', 'EVENTS'))
            else array_construct()
        end as read_columns
    from (
        select
            query_id,
            start_time,
            user_name,
            upper(regexp_substr(query_text, '(^|[ (])from +([A-Za-z0-9_$]+[.][A-Za-z0-9_$]+[.][A-Za-z0-9_$]+)', 1, 1, 'ie', 2)) as read_fqn,
            upper(coalesce(
                regexp_substr(query_text, '(^|[ ])table +([A-Za-z0-9_$]+[.][A-Za-z0-9_$]+[.][A-Za-z0-9_$]+)', 1, 1, 'ie', 2),
                regexp_substr(query_text, '(^|[ ])insert +into +([A-Za-z0-9_$]+[.][A-Za-z0-9_$]+[.][A-Za-z0-9_$]+)', 1, 1, 'ie', 2)
            )) as write_fqn
        from {{ ref('fixture_query_history') }}
    )
    where read_fqn is not null or write_fqn is not null
)
