-- inter_customer_orders_enriched.sql 1row=1order
-- order id with count of jaffles / beverages and count of unique products and total product, customer and store info.
with orders as (
        select order_id, customer_id, order_total_amount, order_date, store_id from {{ ref('inter_customer_orders_core') }}
    ),
    items as (
        select order_id, order_item_id, product_id from {{ ref('stg_order_items') }}
    ),
    products as (
        select product_id, product_type from {{ ref('stg_products') }}
    ),
    store_locations as (
        select store_id, location_name from {{ ref('stg_store_locations') }}
    ),
    order_agg as (
        select order_id,
            sum(case when p.product_type = 'jaffle' then 1 else 0 end) as number_of_jaffles,
            sum(case when p.product_type = 'beverage' then 1 else 0 end) as number_of_beverages,
            count(distinct product_id) as unique_products_count,
            count(order_item_id) as total_items_count
        from items i
            join products p
            using (product_id)
        group by order_id
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

 