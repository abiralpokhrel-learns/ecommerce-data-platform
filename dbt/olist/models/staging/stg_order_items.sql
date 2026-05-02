{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_at,
        price::numeric(12,2) as item_price,
        freight_value::numeric(12,2) as freight_value,
        _loaded_at
    from source
)

select * from renamed