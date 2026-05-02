{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'geolocation') }}
),

aggregated as (
    -- Multiple lat/lng per zip prefix; take the average for a clean lookup
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        avg(geolocation_lat) as latitude,
        avg(geolocation_lng) as longitude,
        max(geolocation_city) as city,
        max(geolocation_state) as state
    from source
    group by 1
)

select * from aggregated