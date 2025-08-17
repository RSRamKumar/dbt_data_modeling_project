
-- customer_orders_summary
select
        customer_id,
        min(order_date)  as first_order_date,
        max(order_date)  as recent_order_date,
        count(*)         as lifetime_orders_count,
        sum(order_total_amount) as lifetime_total_amount,
        datediff('day', first_order_date, recent_order_date) as customer_active_span
    from {{ ref('inter_customer_orders_enriched') }}
    group by customer_id