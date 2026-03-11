{{
    config(
        materialized='table'
    )
}}

with orders as (

    select
        order_key,
        customer_key,
        order_date,
        total_order_price,
        order_status,
        order_priority,
        order_ship_priority,
        order_clerk
    from
        {{ ref('stg_orders') }}

),

line_item as (

    select
        order_key,
        part_key,
        supplier_key,
        line_number,
        line_status,
        ship_date,
        receipt_date,
        commit_date,
        quantity,
        extended_price,
        discount,
        tax,
        return_flag,
        (extended_price * (1 - discount)) as net_item_sales_amount
    from
        {{ ref('stg_lineitem') }}

)

select
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['line_number', 'orders.order_key']) }} as order_item_key,
    
    -- Keys
    line_item.order_key,
    orders.customer_key,
    line_item.part_key,
    line_item.supplier_key,
    line_item.line_number,

    -- Order details
    orders.order_date,
    orders.order_status,
    orders.order_clerk,
    orders.order_priority,
    orders.order_ship_priority,
    line_item.ship_date,
    line_item.receipt_date,
    line_item.commit_date,
    line_item.return_flag,

    -- Line item measures
    line_item.quantity,
    line_item.line_status,
    orders.total_order_price,
    line_item.extended_price,
    line_item.discount,
    line_item.tax,
    line_item.net_item_sales_amount
from
    line_item
left join
    orders on line_item.order_key = orders.order_key