{{ config(materialized='table') }}

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        i.order_item_sk,
        i.order_id,
        i.order_item_id,
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} as customer_sk,
        {{ dbt_utils.generate_surrogate_key(['i.product_id']) }} as product_sk,
        {{ dbt_utils.generate_surrogate_key(['i.seller_id']) }} as seller_sk,
        {{ dbt_utils.generate_surrogate_key(['o.purchased_at::date']) }} as purchase_date_sk,
        i.item_price,
        i.freight_value,
        i.item_price + i.freight_value as total_value,
        o.purchased_at,
        o.order_status
    from items i
    left join orders o on i.order_id = o.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final