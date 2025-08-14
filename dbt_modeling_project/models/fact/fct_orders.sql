with enriched_orders as (
    select
        order_id,
        order_date,
        customer_id,
        order_total_amount,
        number_of_jaffles,
        number_of_beverages,
        unique_products_count,
        total_items_count,
        store_id
    from {{ ref('inter_customer_orders_enriched') }}  
),
dim_customers as (
    select customer_sk, customer_id, effective_date from {{ ref('dim_customers') }} 
),
dim_dates as (
    select date_actual, date_sk from {{ ref('dim_dates') }} 
)
select 
    order_id,
        order_date,
        date_sk,
        customer_sk,
        order_total_amount,
        number_of_jaffles,
        number_of_beverages,
        unique_products_count,
        total_items_count,
        store_id

    from enriched_orders eo join dim_customers dc using(customer_id) 
    join dim_dates dd on eo.order_date = dd.date_actual

 
   
