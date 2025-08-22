-- store_sales_summary

with orders as (
    select
        order_id,
        customer_sk,
        store_sk,
        order_total_amount,
        total_items_count
    from {{ ref('fct_orders') }}
)

select 
    o.store_sk,

    -- Core metrics
    sum(o.order_total_amount)      as total_sales_amount,
    sum(o.total_items_count)       as total_items_sold,
    count(distinct o.order_id)     as total_orders_count,
    count(distinct o.customer_id)  as unique_customers_count,

    -- KPI metrics
    round(sum(o.order_total_amount) / nullif(count(distinct o.order_id), 0), 2) as avg_order_value,
    round(sum(o.total_items_count) / nullif(count(distinct o.order_id), 0), 2)  as avg_items_per_order,

    current_timestamp() as load_timestamp 

from orders o 
group by store_sk 

