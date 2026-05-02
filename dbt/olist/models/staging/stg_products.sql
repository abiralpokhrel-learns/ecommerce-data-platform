{{ config(materialized='view') }}

with products as (
    select * from {{ source('raw', 'products') }}
),

translations as (
    select * from {{ source('raw', 'product_category_translation') }}
),

renamed as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name) as product_category,
        p.product_name_lenght as product_name_length,
        p.product_description_lenght as product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p._loaded_at
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
)

select * from renamed