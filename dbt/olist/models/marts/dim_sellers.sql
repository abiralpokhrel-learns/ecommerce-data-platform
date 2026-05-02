{{ config(materialized='table') }}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

geo as (
    select * from {{ ref('stg_geolocation') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }} as seller_sk,
        s.seller_id,
        s.zip_code_prefix,
        s.city,
        s.state,
        g.latitude,
        g.longitude,
        current_timestamp as dbt_updated_at
    from sellers s
    left join geo g on s.zip_code_prefix = g.zip_code_prefix
)

select * from final