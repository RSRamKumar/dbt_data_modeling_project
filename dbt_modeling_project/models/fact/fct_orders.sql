with enriched_orders as (
    select
        o.order_id,
        o.order_date,
        o.customer_id,
        o.order_total_amount,
        o.number_of_jaffles,
        o.number_of_beverages,
        o.unique_products_count,
        o.total_items_count,
        o.store_id
    from {{ ref('inter_customer_orders_enriched') }} o
),

 
fact_base as (
    select
        eo.order_id,
        eo.order_date as date_id,  -- Natural key from date_dim
        dc.customer_sk,
        eo.order_total_amount,
        eo.number_of_jaffles,
        eo.number_of_beverages,
        eo.unique_products_count,
        eo.total_items_count
    from enriched_orders eo
    left join {{ ref('dim_customers') }} dc
      on eo.customer_id = dc.customer_id
     and eo.order_date between dc.effective_date and coalesce(dc.end_date, current_date)
    left join {{ ref('dim_stores') }} ds
      on eo.store_id = ds.store_id
 )

select
    *,
    current_timestamp() as load_timestamp
from fact_base

