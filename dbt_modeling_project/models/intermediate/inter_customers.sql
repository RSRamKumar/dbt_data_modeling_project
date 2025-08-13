-- inter_customers.sql   1 row = 1 customer
with customer_orders as (
    select 
        customer_id,
        customer_name,
        order_date,
        order_total_amount
    from {{ ref('inter_customer_orders_enriched') }}
)
select
    customer_id,
    customer_name,
    min(order_date)        as first_order_date,
    max(order_date)        as recent_order_date,
    count(*)               as total_orders_count,
    sum(order_total_amount) as total_order_amount,  -- count(order_item_id) * product_price
    avg(order_total_amount) as avg_order_value,
    datediff('day', min(order_date), max(order_date)) as customer_active_span,
    -- current_date-max(order_date) as recency_metric
from customer_orders
group by customer_id, customer_name