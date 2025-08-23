-- product_sales_summary
-- 1 row = 1 product

with order_products as (
        select
            product_sk,
            product_price,
            quantity,
            store_sk
        from  {{ ref('fct_order_products') }}
    ),
    dim_products as (
        select product_sk, product_id, product_name, product_type from {{ ref('dim_products') }}
    )

    select 
        op.product_sk,
        dp.product_name, 
        sum(op.quantity) as total_quantity_sold, 
        sum(op.quantity * op.product_price) as product_revenue
    from order_products op 
    join dim_products dp 
    on op.product_sk = dp.product_sk
    group by  op.product_sk, dp.product_name 
