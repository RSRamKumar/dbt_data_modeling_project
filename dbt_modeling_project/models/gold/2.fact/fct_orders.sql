-- fact_orders.sql one row per order
-- Fact table with surrogate keys for customer, product
with enriched_orders as (
        select
            order_id,
            order_date,
            customer_id,
            order_total_amount,
            number_of_jaffles,
            number_of_beverages,
            unique_products_count,
            total_items_count,
            store_id
        from {{ ref('inter_orders_enriched') }}
    ),
    dim_customers as (
        select customer_sk, customer_id from {{ ref('dim_customers') }}
    ),
    dim_dates as (
        select date_actual, date_sk from {{ ref('dim_dates') }}
    ),
    dim_stores as (
        select store_sk, store_id from {{ ref('dim_stores') }}
    )
select
    eo.order_id,
    dd.date_sk,
    dc.customer_sk,
    eo.order_total_amount,
    eo.number_of_jaffles,
    eo.number_of_beverages,
    eo.unique_products_count,
    eo.total_items_count,
    ds.store_sk,
    current_timestamp() as load_timestamp

from enriched_orders eo
    join dim_dates dd on eo.order_date = dd.date_actual
    join dim_customers dc on eo.customer_id = dc.customer_id
    join dim_stores ds on eo.store_id = ds.store_id
   
   
   -- and eo.order_date between dc.effective_date and coalesce(dc.end_date, current_date)
   -- If SCD is tracked, then it can be considered. 
   -- If a customer changes name, email, or other tracked attributes, 
   -- the fact row will still link to the correct customer version at that order’s date.
