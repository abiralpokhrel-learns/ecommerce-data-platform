{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'order_payments') }}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value::numeric(12,2) as payment_value,
        _loaded_at
    from source
)

select * from renamed