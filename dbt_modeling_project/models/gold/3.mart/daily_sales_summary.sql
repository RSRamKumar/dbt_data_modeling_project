-- daily_sales_summary.sql
-- Grain: 1 row per store_id per order_date

with orders as (
    select
        order_id,
        customer_sk,
        store_sk,
        date_sk,
        order_total_amount,
        total_items_count
    from {{ ref('fct_orders') }}
)

select
    o.date_sk,
    o.store_sk,

    -- Core metrics
    sum(o.order_total_amount) as total_sales_amount,
    sum(o.total_items_count) as total_items_sold,
    count(distinct o.order_id) as total_orders_count,
    count(distinct o.customer_sk) as unique_customers_count,

    -- KPI metrics
    round(sum(o.order_total_amount) / nullif(count(distinct o.order_id), 0), 2) as avg_order_value,
    round(sum(o.total_items_count) / nullif(count(distinct o.order_id), 0), 2) as avg_items_per_order

from orders o
group by o.date_sk, o.store_sk



 