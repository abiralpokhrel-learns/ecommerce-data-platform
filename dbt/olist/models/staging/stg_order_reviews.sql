{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'order_reviews') }}
),

deduplicated as (
    -- Some review_ids appear multiple times. Keep latest.
    select
        *,
        row_number() over (partition by review_id order by review_creation_date desc) as rn
    from source
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::timestamp as created_at,
        review_answer_timestamp::timestamp as answered_at,
        _loaded_at
    from deduplicated
    where rn = 1
)

select * from renamed