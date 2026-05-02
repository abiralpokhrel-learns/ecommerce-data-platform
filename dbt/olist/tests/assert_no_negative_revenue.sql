-- Returns rows that fail the test
select order_id, payment_total
from {{ ref('fct_orders') }}
where payment_total < 0