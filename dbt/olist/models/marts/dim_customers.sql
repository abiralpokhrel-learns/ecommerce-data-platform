{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

geo as (
    select * from {{ ref('stg_geolocation') }}
),

-- Customer_id is per-order in this dataset; customer_unique_id is the real customer
dedup as (
    select
        customer_unique_id,
        max(customer_id) as latest_customer_id,
        max(zip_code_prefix) as zip_code_prefix,
        max(city) as city,
        max(state) as state
    from customers
    group by 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['d.customer_unique_id']) }} as customer_sk,
        d.customer_unique_id,
        d.latest_customer_id,
        d.zip_code_prefix,
        d.city,
        d.state,
        g.latitude,
        g.longitude,
        current_timestamp as dbt_updated_at
    from dedup d
    left join geo g on d.zip_code_prefix = g.zip_code_prefix
)

select * from final