-- fact_orders.sql one row per order
-- Fact table with surrogate keys for customer, product
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
        select customer_sk, customer_id, effective_date, end_date from {{ ref('dim_customers') }}
    ),
    dim_dates as (
        select date_actual, date_sk from {{ ref('dim_dates') }}
    )
select
    eo.order_id,
    dd.date_sk,
    dc.customer_sk,
    eo.order_total_amount,
    eo.number_of_jaffles,
    eo.number_of_beverages,
    eo.unique_products_count,
    eo.total_items_count,
    eo.store_id,
    current_timestamp() as load_timestamp

from enriched_orders eo
    join dim_dates dd on eo.order_date = dd.date_actual
    join dim_customers dc on eo.customer_id = dc.customer_id
    and eo.order_date between dc.effective_date and coalesce(dc.end_date, current_date)
