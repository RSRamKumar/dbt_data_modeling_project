
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



-- Top 5 loyal customers (by number of orders)
select customer_name, count(distinct fo.order_id) as number_of_orders, dense_rank() over(order by count(distinct fo.order_id) desc) as rank 
from fct_orders fo  
join dim_customers dc 
using (customer_sk)
group by customer_name
qualify rank <= 5


select customer_name, lifetime_orders_count as number_of_orders, dense_rank() over(order by lifetime_orders_count desc) as rank
from {{ ref('customer_orders_summary') }} 
qualify rank <= 5
 

-- Top 5 customers with highest sales
select customer_name, sum(order_total_amount) as total_order_amount, dense_rank() over (order by sum(order_total_amount) desc ) as rank
from fct_orders fo 
join dim_customers dc 
using(customer_sk)
group by customer_name
qualify rank <= 5


select * from {{ ref('customer_orders_summary') }} limit 5



-- Debug Pathways
select order_id, count(*) 
from fct_orders
group by order_id
having count(*) > 1;



select order_id, count(*) 
from inter_orders_enriched
group by order_id
having count(*) > 1;


-- Step 3: check if any join (dates/customers/stores) multiplies rows
with x as (
  select
    eo.order_id,
    dd.date_sk,
    dc.customer_sk,
    ds.store_sk
  from {{ ref('inter_orders_enriched') }} eo
  join {{ ref('dim_dates') }}     dd on eo.order_date  = dd.date_actual
  join {{ ref('dim_customers') }} dc on eo.customer_id = dc.customer_id
  join {{ ref('dim_stores') }}    ds on eo.store_id    = ds.store_id
)
select order_id, count(*) as cnt
from x
group by order_id
having count(*) > 1
order by cnt desc, order_id;

