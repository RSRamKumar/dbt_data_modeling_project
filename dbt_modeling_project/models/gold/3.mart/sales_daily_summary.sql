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

    -- Core metrics and KPI metrics
    {{ order_metrics("o") }},

    current_timestamp() as load_timestamp

from orders o
group by o.date_sk, o.store_sk



