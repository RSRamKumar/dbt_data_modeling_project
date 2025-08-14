 

-- former inter_customer.sql and now dim_customers.sql   1 row = 1 customer
with customer_orders as (
    select 
        customer_id,
        customer_name,
        order_date,
        order_total_amount
    from {{ ref('inter_customer_orders_enriched') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_name']) }} as customer_sk,
    customer_id,
    customer_name,
    min(order_date)        as first_order_date,
    max(order_date)        as recent_order_date,
    count(*)               as lifetime_orders_count,
    sum(order_total_amount) as lifetime_total_amount,   -- count(order_item_id) * product_price
    datediff('day', min(order_date), max(order_date)) as customer_active_span,  
    
    min(order_date) as effective_date,
    null::date as end_date,
    true as is_current,
    current_timestamp() as load_timestamp 

from customer_orders
group by customer_sk, customer_id, customer_name