

-- Top selling Product by revenue
select dp.product_name, dense_rank() over (order by  sum(fcp.item_total) desc) as rank
from {{ ref('fct_order_products') }}  fcp
join dim_products dp 
using(product_sk)
group by  fcp.product_sk, dp.product_name
qualify rank <= 3

-- Top 3 selling product by NUMBER_OF_ITEMS_SOLD
with top_selling_product as (
        select *, dense_rank() over (partition by product_type order by NUMBER_OF_ITEMS_SOLD desc) as rank
        from {{ ref('inter_product_sales_agg') }}
    )
select product_id, product_name, product_type, NUMBER_OF_ITEMS_SOLD, rank from top_selling_product where rank <= 3

-- Top 5 loyal customers
select customer_id, customer_name, count(order_id) number_of_orders
from {{ ref('inter_orders_enriched') }}
group by customer_id, customer_name
order by number_of_orders desc limit 5



-- Top 5 customers with highest sales
select customer_id, customer_name, sum(order_total_amount) total_purchase_amount
from {{ ref('inter_orders_enriched') }}
group by customer_id, customer_name
order by total_purchase_amount desc limit 5
