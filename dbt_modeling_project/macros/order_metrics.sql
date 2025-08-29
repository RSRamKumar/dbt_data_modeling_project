{% macro order_metrics(table_alias) %}
    -- Core metrics
    sum({{ table_alias }}.order_total_amount) as total_sales_amount,
    sum({{ table_alias }}.total_items_count) as total_items_sold,
    count(distinct {{ table_alias }}.order_id) as total_orders_count,
    count(distinct {{ table_alias }}.customer_sk) as unique_customers_count,

    -- KPI metrics
    round(sum({{ table_alias }}.order_total_amount) / nullif(count(distinct {{ table_alias }}.order_id), 0), 2) as avg_order_value,
    round(sum({{ table_alias }}.total_items_count) / nullif(count(distinct {{ table_alias }}.order_id), 0), 2) as avg_items_per_order
{% endmacro %}