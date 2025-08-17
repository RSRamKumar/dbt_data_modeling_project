-- inter_orders_enriched.sql 1row=1order
-- order id with count of jaffles / beverages and count of unique products and total product, customer and store info.
with orders as (
        select order_id, customer_id, order_total_amount, order_date, store_id from {{ ref('stg_orders') }}
    ),

    order_agg as (
        select order_id, number_of_jaffles, number_of_beverages, unique_products_count, total_items_count from {{ ref('inter_order_items_agg') }}

    ),
    store_locations as (
        select store_id, location_name from {{ ref('stg_store_locations') }}
    ),

    customers as (
        select customer_id, customer_name from {{ ref('stg_customers') }}
    )
select o.order_id, o.order_date, o.customer_id, c.customer_name, o.order_total_amount,
    og.number_of_jaffles, og.number_of_beverages, og.unique_products_count, og.total_items_count,
    o.store_id, s.location_name

from orders o

    join order_agg og
    using (order_id)

    join customers c
    using (customer_id)

    join store_locations s
    using (store_id)