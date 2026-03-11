{{
    config(
        materialized='table'
    )
}}

with customers as (

    select
        customer_key,
        customer_name,
        customer_address,
        nation_key,
        customer_phone_number,
        customer_account_balance,
        customer_market_segment
    from
        {{ ref('stg_customers') }}

),

nations as (

    select
        nation_key,
        nation,
        region_key
    from
        {{ ref('stg_nations') }}

),

regions as (

    select
        region_key,
        region
    from
        {{ ref('stg_regions') }}

),

final as (

    select
        customers.customer_key,
        customers.customer_name,
        customers.customer_address,
        customers.customer_phone_number,
        customers.customer_account_balance,
        customers.customer_market_segment,
        nations.nation,
        nations.nation_key,
        regions.region,
        regions.region_key
    from
        customers
    left join
        nations on customers.nation_key = nations.nation_key
    left join
        regions on nations.region_key = regions.region_key

)

select * from final