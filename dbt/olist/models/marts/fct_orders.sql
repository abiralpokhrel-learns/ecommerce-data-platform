{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*) as item_count,
        sum(item_price) as items_total,
        sum(freight_value) as freight_total
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select
        order_id,
        sum(payment_value) as payment_total,
        max(payment_installments) as max_installments
    from {{ ref('stg_order_payments') }}
    group by 1
),

reviews as (
    select
        order_id,
        avg(review_score)::numeric(3,2) as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by 1
),

customers as (
    select
        customer_id,
        customer_unique_id
    from {{ ref('stg_customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_sk,
        o.order_id,
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} as customer_sk,
        c.customer_unique_id,
        o.order_status,
        
        -- Dates
        o.purchased_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,
        o.purchased_at::date as purchase_date,
        
        -- Date FK to dim_dates
        {{ dbt_utils.generate_surrogate_key(['o.purchased_at::date']) }} as purchase_date_sk,
        
        -- Measures
        coalesce(i.item_count, 0) as item_count,
        coalesce(i.items_total, 0) as items_total,
        coalesce(i.freight_total, 0) as freight_total,
        coalesce(p.payment_total, 0) as payment_total,
        coalesce(p.max_installments, 0) as max_installments,
        r.avg_review_score,
        
        -- Calculated
        case
            when o.delivered_to_customer_at is not null
            then extract(day from o.delivered_to_customer_at - o.purchased_at)
        end as days_to_delivery,
        
        case
            when o.delivered_to_customer_at is not null
            then extract(day from o.delivered_to_customer_at - o.estimated_delivery_at)
        end as days_late,
        
        case
            when o.delivered_to_customer_at > o.estimated_delivery_at then true
            when o.delivered_to_customer_at is not null then false
        end as is_late,
        
        current_timestamp as dbt_updated_at
        
    from orders o
    left join items i on o.order_id = i.order_id
    left join payments p on o.order_id = p.order_id
    left join reviews r on o.order_id = r.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final