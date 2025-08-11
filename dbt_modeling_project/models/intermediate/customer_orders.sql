

select order_id, customer_id, sum(order_total) AS total_sales, count(distinct PRODUCT_ID) as number_of_unique_products,
count( order_item_id) as number_of_orders
from JAFFLE.DEV_SCHEMA.STG_ORDERS 
join JAFFLE.DEV_SCHEMA.STG_ORDER_ITEMS
using(order_id)
group by order_id, customer_id 
order by total_sales desc 