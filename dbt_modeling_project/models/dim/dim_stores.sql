
-- former inter_stores_enriched.sql and now dim_stores.sql

with customer_orders as ( 
    select 
        store_id, 
        location_name, 
        order_total_amount, 
        total_items_count, 
        order_id, 
        customer_id 
    from {{ ref('inter_customer_orders_enriched') }}
)

select 
    store_id,
    location_name,

    -- Core metrics
    sum(order_total_amount)      as total_sales_amount,
    sum(total_items_count)       as total_items_sold,
    count(distinct order_id)     as total_orders_count,
    count(distinct customer_id)  as unique_customers_count,

    -- KPI metrics
    round(sum(order_total_amount) / nullif(count(distinct order_id), 0), 2) as avg_order_value,
    round(sum(total_items_count) / nullif(count(distinct order_id), 0), 2)  as avg_items_per_order,

    current_date as effective_date,
    current_timestamp() as load_timestamp 

from customer_orders
group by store_id, location_name
-- order by total_sales_amount desc
