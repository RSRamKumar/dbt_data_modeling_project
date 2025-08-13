-- inter_customer_orders_core.sql
-- Grain: 1 row per order_id


with orders as (
        select * from {{ ref('stg_orders') }}
    )
select
    order_id,
    customer_id,
    order_total_amount,
    order_date,
    store_id,
    current_timestamp() as load_timestamp
from orders