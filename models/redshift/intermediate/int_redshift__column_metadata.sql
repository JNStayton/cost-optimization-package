{{ config(materialized='table') }}

{# 1:1 table over stg_redshift__column_metadata. Lands leader-only SVV data on
   compute nodes so downstream models can join uniformly compute-side. #}

select * from {{ ref('stg_redshift__column_metadata') }}
