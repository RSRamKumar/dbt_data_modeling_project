
-- Top selling Product by revenue
select dp.product_name, dense_rank() over (order by sum(fcp.item_total) desc) as rank
from {{ ref('fct_order_products') }} fcp
    join {{ ref('dim_products') }} dp
    using (product_sk)
group by dp.product_name
    qualify rank <= 3



-- Top 3 selling product by number of items sold
select dp.product_name, dense_rank() over (order by sum(quantity) desc) as rank
from fct_order_products fcp
    join dim_products dp
    using (product_sk)
group by dp.product_name
    qualify rank <= 3



-- Top 5 loyal customers (by number of orders)
select dc.customer_id, dc.customer_name,
    count(distinct fo.order_id) as number_of_orders,
    dense_rank() over (order by count(distinct fo.order_id) desc) as rank
from fct_orders fo
    join dim_customers dc using (customer_sk)
group by dc.customer_id, dc.customer_name
    qualify rank <= 5

-- Another method directly from mart
select customer_name, lifetime_orders_count as number_of_orders,
    dense_rank() over (order by lifetime_orders_count desc) as rank
from {{ ref('customer_orders_summary') }}
    qualify rank <= 5


-- Top 5 customers with highest sales
select dc.customer_id, dc.customer_name, sum(order_total_amount) as total_order_amount,
    dense_rank() over (order by sum(order_total_amount) desc) as rank
from fct_orders fo
    join dim_customers dc
    using (customer_sk)
group by dc.customer_id, dc.customer_name
    qualify rank <= 5

-- Another method directly from mart
select customer_name, lifetime_total_amount,
    dense_rank() over (order by lifetime_total_amount desc) as rank
from {{ ref('customer_orders_summary') }}
    qualify rank <= 5



-- store had the highest total sales in 2020
select 
    ds.store_sk,
    sum(dss.total_sales_amount) as yearly_sales
from {{ ref('sales_daily_summary') }} dss
join {{ ref('dim_stores') }} ds using(store_sk)
join {{ ref('dim_dates') }} dd using(date_sk)
where dd.year = 2020  
group by ds.store_sk
order by yearly_sales desc
 


-- customers and no. of shops they visited
select customer_sk, count(distinct store_sk)
from fct_orders 
group by customer_sk
having count(distinct store_sk) > 1


-- Monthly Sales Trend
select dd.month_name, sum(fo.order_total_amount) as monthly_sales
from fct_orders fo
join dim_dates dd on fo.date_sk = dd.date_sk
group by dd.month_name
order by monthly_sales desc;
