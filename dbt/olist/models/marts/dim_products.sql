{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
        product_id,
        coalesce(product_category, 'unknown') as product_category,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        (product_weight_g / 1000.0) as product_weight_kg,
        (product_length_cm * product_height_cm * product_width_cm) as product_volume_cm3,
        product_photos_qty,
        current_timestamp as dbt_updated_at
    from source
)

select * from final