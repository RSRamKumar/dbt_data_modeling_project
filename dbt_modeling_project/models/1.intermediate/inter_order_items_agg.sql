-- inter_order_items_agg
-- Aggregates order → counts of jaffles, beverages, unique products, items.


with
    order_items as (
        select order_id, order_item_id, product_id from {{ ref('stg_order_items') }}
    ),
    products as (
        select product_id, product_type from {{ ref('stg_products') }}
    )

select order_id,
    sum(case when p.product_type = 'jaffle' then 1 else 0 end) as number_of_jaffles,
    sum(case when p.product_type = 'beverage' then 1 else 0 end) as number_of_beverages,
    count(distinct product_id) as unique_products_count,
    count(order_item_id) as total_items_count
from order_items oi
    join products p
    using (product_id)
group by order_id