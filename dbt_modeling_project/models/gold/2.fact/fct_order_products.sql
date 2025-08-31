
-- fct_order_items.sql 
-- grain =  1 row per (order_id, product_id)  -- one record per unique product within an order


  with order_items as (
          select order_id, product_id from {{ ref('stg_order_items') }}
      ),
      orders as (
          select order_id, order_date, customer_id, store_id from {{ ref('stg_orders') }}
      ),
      dim_dates as (
          select date_actual, date_sk from {{ ref('dim_dates') }}
      ),
      dim_products as (
          select product_sk, product_id, product_price,   from {{ ref('dim_products') }}
      ),
      dim_customers as (
          select customer_sk, customer_id  from {{ ref('dim_customers') }}
      ),
      dim_stores as (
        select store_sk, store_id from {{ ref('dim_stores') }}
    )



  select o.order_id, dd.date_sk, dc.customer_sk, dp.product_sk, ds.store_sk, count(*) as quantity,
      dp.product_price, count(*) * dp.product_price as item_total

  from order_items i
      join orders o
      on i.order_id = o.order_id
      join dim_customers dc on
      o.customer_id = dc.customer_id
    --  and o.order_date between dc.effective_date and coalesce(dc.end_date, current_date)
      join dim_dates dd
      on o.order_date = dd.date_actual
      join dim_products dp
      on i.product_id = dp.product_id
      -- and o.order_date between dp.effective_date and coalesce(dp.end_date, current_date)
      join dim_stores ds 
      on o.store_id = ds.store_id

 -- If order date is not between product effective date (date of insertion of products), query produces no results


  group by o.order_id,
      dd.date_sk,
      dc.customer_sk,
      dp.product_sk,
      ds.store_sk,
      dp.product_price

   




 
--For quantity
--If in the future stg_order_items includes a quantity field (e.g., a customer buys 3 coffees in one order item line)
--SUM(COALESCE(i.quantity, 1)) as quantity  