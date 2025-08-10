

With raw_customers as (
    select * from JAFFLE.RAW_SCHEMA.RAW_CUSTOMERS
)

select 
    id as customer_id,
    name as customer_name,
    current_timestamp as load_timestamp
 from raw_customers 



