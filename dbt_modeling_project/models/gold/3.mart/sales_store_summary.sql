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

    -- Core metrics and KPI metrics
   {{ order_metrics("o") }},

    current_timestamp() as load_timestamp

from orders o
    join dim_stores ds using (store_sk)
group by o.store_sk, ds.location_name
