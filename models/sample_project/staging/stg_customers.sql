{{ config(materialized='view') }}

with source as (

    select * from {{ source('unclustered', 'raw_customer') }}

),

renamed as (

    select
        c_custkey as customer_key,
        c_name as customer_name,
        c_address as customer_address,
        c_nationkey as nation_key,
        c_phone as customer_phone_number,
        c_acctbal as customer_account_balance,
        c_mktsegment as customer_market_segment,
        c_comment as customer_comment

    from source

)

select * from renamed