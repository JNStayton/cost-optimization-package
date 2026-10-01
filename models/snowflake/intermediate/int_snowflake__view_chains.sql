{{
  config(
    materialized='table',
  )
}}

{#-- One row per view or ephemeral that feeds at least one table through views and
     ephemerals only. Built from int_snowflake__view_chain_pairs. --#}

select
    upstream_fqn                    as model_fqn,
    count(distinct table_fqn)       as downstream_table_count,
    array_agg(distinct table_fqn)   as downstream_table_fqns,
    min(path_length)                as min_hops_to_table
from {{ ref('int_snowflake__view_chain_pairs') }}
group by upstream_fqn
