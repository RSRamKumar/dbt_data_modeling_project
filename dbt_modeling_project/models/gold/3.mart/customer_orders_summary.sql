
-- customer_orders_summary

with customer_summary_base as (
        select
            customer_id,
            min(order_date) as first_order_date,
            max(order_date) as recent_order_date,
            count(*) as lifetime_orders_count,
            sum(order_total_amount) as lifetime_total_amount,
            datediff('day', first_order_date, recent_order_date) as customer_active_span
        from {{ ref('inter_orders_enriched') }}
        group by customer_id
    ),
    dim_customers as (
        select customer_sk, customer_id, customer_name from {{ ref('dim_customers') }}
    )

select
    customer_id,
    customer_sk,
    customer_name,
    first_order_date,
    recent_order_date,
    lifetime_orders_count,
    lifetime_total_amount,
    customer_active_span

from customer_summary_base
    join dim_customers

    using (customer_id)


