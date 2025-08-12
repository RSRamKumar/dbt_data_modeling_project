

with orders
    as (

        select
            * from {{ ref('stg_orders') }}

    ),

    items as (

        select
            * from {{ ref('stg_order_items') }}

    )

select

    o.order_id,

    o.customer_id,

    o.order_total as total_sales,

    count(distinct i.product_id)
    as unique_products_count,

    count(i.order_item_id)
    as total_items_count,

    o.ordered_at as order_date

from orders o

    join items i
    using (order_id)
group by all
order by total_sales desc 