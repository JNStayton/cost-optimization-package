{{ config(materialized='table') }}

with advisor_recommendations as (

    select
        database_name,
        table_id,
        group_id,
        recommended_ddl,
        is_auto_eligible

    from {{ ref('int_redshift__alter_table_recommendations') }}
    where recommendation_type = 'sortkey'

),

table_info as (

    select
        database_name,
        schema_name,
        table_name,
        table_id,
        size_mb,
        sortkey1,
        sortkey_num

    from {{ ref('int_redshift__table_info') }}

)

select
    t.database_name,
    t.schema_name,
    t.table_name,
    t.size_mb,
    t.sortkey1                                  as current_sortkey1,
    t.sortkey_num                               as current_sortkey_num,
    'sort: ['''
        || replace(
            trim(split_part(split_part(r.recommended_ddl, 'SORTKEY (', 2), ')', 1)),
            ', ',
            ''', '''
        )
        || ''']'                                as recommended_dbt_config,
    r.group_id,
    r.is_auto_eligible,
    coalesce(t.size_mb, 0)::float               as priority_score,
    'advisor'                                   as source

from advisor_recommendations r
inner join table_info t
    on r.table_id = t.table_id
