-- customer_orders_summary.sql (gold mart)
with customer_summary_base as (
    select
        fo.customer_sk,
        min(dd.date_actual) as first_order_date,
        max(dd.date_actual) as recent_order_date,
        count(distinct fo.order_id) as lifetime_orders_count,
        sum(fo.order_total_amount) as lifetime_total_amount,
        datediff('day', min(dd.date_actual), max(dd.date_actual)) as customer_active_span
    from {{ ref('fct_orders') }} fo
    join {{ ref('dim_dates') }} dd using(date_sk)
    group by fo.customer_sk
)
select
    csb.customer_sk,
    dc.customer_id,
    dc.customer_name,
    csb.first_order_date,
    csb.recent_order_date,
    csb.lifetime_orders_count,
    csb.lifetime_total_amount,
    csb.customer_active_span
from customer_summary_base csb
join {{ ref('dim_customers') }} dc using (customer_sk)




{# Mart Naming Convention
Prefix by subject / domain:
Eg: sales, customer, product, store

Suffix by grain:
Eg: _daily (1 row per day), _monthly (1 row per month), _summary (1 row per entity), _trend (time-series oriented mart) #}