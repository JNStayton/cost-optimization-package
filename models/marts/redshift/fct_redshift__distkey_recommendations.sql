{{ config(materialized='table') }}

with advisor_recommendations as (

    select
        database_name,
        table_id,
        group_id,
        recommended_ddl,
        is_auto_eligible

    from {{ ref('int_redshift__alter_table_recommendations') }}
    where recommendation_type = 'distkey'

),

table_info as (

    select
        database_name,
        schema_name,
        table_name,
        table_id,
        size_mb,
        distribution_style

    from {{ ref('int_redshift__table_info') }}

)

select
    t.database_name,
    t.schema_name,
    t.table_name,
    t.size_mb,
    t.distribution_style                        as current_diststyle,
    case
        when r.recommended_ddl ilike '%DISTSTYLE EVEN%'
            then 'dist: ''even'''
        when r.recommended_ddl ilike '%DISTSTYLE ALL%'
            then 'dist: ''all'''
        when r.recommended_ddl ilike '%DISTKEY (%'
            then 'dist: '''
                || trim(split_part(split_part(r.recommended_ddl, 'DISTKEY (', 2), ')', 1))
                || ''''
    end                                         as recommended_dbt_config,
    r.group_id,
    r.is_auto_eligible,
    coalesce(t.size_mb, 0)::float               as priority_score,
    'advisor'                                   as source

from advisor_recommendations r
inner join table_info t
    on r.table_id = t.table_id
