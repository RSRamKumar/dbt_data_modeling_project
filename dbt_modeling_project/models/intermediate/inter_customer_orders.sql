
with customers as (

        select
            customer_id, customer_name from {{ ref('stg_customers') }}

    ),

    orders
    as (

        select
            order_id, ordered_at, customer_id, order_total, store_id from {{ ref('stg_orders') }}

    ),

    items as (

        select
            order_item_id, order_id, product_id from {{ ref('stg_order_items') }}

    ),

    products as (

        select
            product_id, product_type from {{ ref('stg_products') }}

    ),

    store_locations as (
        select store_id, location_name from {{ ref('stg_store_locations') }}
    )

select

    o.order_id,

    o.ordered_at as order_date,

    o.customer_id,

    c.customer_name,



    o.order_total as order_total,

    count(distinct i.product_id)
    as unique_products_count,

    count(i.order_item_id)
    as total_items_count,


    sum(case when p.product_type = 'jaffle' then 1 else 0 end) as number_of_jaffles,


    sum(case when p.product_type = 'beverage' then 1 else 0 end) as number_of_beverages,

    o.store_id,

    s.location_name,


    current_timestamp() as load_timestamp


from orders o

    join items i
    using (order_id)

    join products p
    using (product_id)

    join customers c
    using (customer_id)

    join store_locations s
    using (store_id)

group by all
order by order_total desc
