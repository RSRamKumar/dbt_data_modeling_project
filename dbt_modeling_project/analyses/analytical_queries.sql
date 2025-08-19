

-- Top selling Product by revenue
select dp.product_name, dense_rank() over (order by  sum(fcp.item_total) desc) as rank
from {{ ref('fct_order_products') }}  fcp
join {{ ref('dim_products') }}  dp 
using(product_sk)
group by dp.product_name 
qualify rank <= 3

-- Top 3 selling product by number of items sold
select dp.product_name, dense_rank() over (order by sum(quantity) desc) as rank
from fct_order_products  fcp
join dim_products dp 
using(product_sk)
group by dp.product_name
qualify rank <= 3

-- Top 5 loyal customers
select customer_name, count(*), dense_rank() over(order by count(*) desc) as rank 
from fct_orders fc   
join dim_customers dc 
using (customer_sk)
group by customer_name
qualify rank <= 5
 

-- Top 5 customers with highest sales
select customer_id, customer_name, sum(order_total_amount) total_purchase_amount
from {{ ref('inter_orders_enriched') }}
group by customer_id, customer_name
order by total_purchase_amount desc limit 5
