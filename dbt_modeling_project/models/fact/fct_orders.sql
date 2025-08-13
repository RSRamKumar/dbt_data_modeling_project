

with customer_order_enriched as (
    select ORDER_ID, 
    ORDER_DATE, 
    CUSTOMER_ID, 
    ORDER_TOTAL_AMOUNT, 
    NUMBER_OF_JAFFLES, 
    NUMBER_OF_BEVERAGES, 
    UNIQUE_PRODUCTS_COUNT, 
    TOTAL_ITEMS_COUNT, 
    STORE_ID 

    from {{ ref('inter_customer_orders_enriched') }}
)

select * , current_timestamp() as load_timestamp from customer_order_enriched 