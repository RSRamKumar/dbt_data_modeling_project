-- inter_products_enriched.sql  
-- 1 row = 1 product (with sales metrics)

with items as (
    select 
        oi.order_item_id, 
        oi.product_id
    from {{ ref('stg_order_items') }} oi
),
products as (
    select 
        p.product_id, 
        p.product_name, 
        p.product_type, 
        p.product_price
    from {{ ref('stg_products') }} p
),
item_sales as (
    -- each row in stg_order_items represents 1 unit sold
    select 
        i.product_id,
        count(i.order_item_id) as number_of_items_sold,
        sum(p.product_price) as total_revenue  -- historical accuracy: summing the actual sold prices
    from items i
    join products p
        on i.product_id = p.product_id
    group by i.product_id
)

select
    p.product_id,
    p.product_name,
    p.product_type,
    p.product_price,
    s.number_of_items_sold,
    s.total_revenue
from products p
left join item_sales s
    on p.product_id = s.product_id
order by s.total_revenue desc
