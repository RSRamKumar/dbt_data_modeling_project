-- store_sales_summary 
-- 1 row = 1 store and metrics


with orders as (
    select
        order_id,
        customer_sk,
        store_sk,
        order_total_amount,
        total_items_count
    from {{ ref('fct_orders') }}
),
dim_stores as (
    select store_sk, store_id, location_name from {{ ref('dim_stores') }}
)

select 
    o.store_sk,
    ds.location_name,

    -- Core metrics
    sum(o.order_total_amount)      as total_sales_amount,
    sum(o.total_items_count)       as total_items_sold,
    count(distinct o.order_id)     as total_orders_count,
    count(distinct o.customer_sk)  as unique_customers_count,

    -- KPI metrics
    round(sum(o.order_total_amount) / nullif(count(distinct o.order_id), 0), 2) as avg_order_value,
    round(sum(o.total_items_count) / nullif(count(distinct o.order_id), 0), 2)  as avg_items_per_order,

    current_timestamp() as load_timestamp 

from orders o 
join dim_stores ds using(store_sk)
group by o.store_sk, ds.location_name
