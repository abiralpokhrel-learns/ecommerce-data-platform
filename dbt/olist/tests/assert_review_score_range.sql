select review_id, review_score
from {{ ref('stg_order_reviews') }}
where review_score not between 1 and 5