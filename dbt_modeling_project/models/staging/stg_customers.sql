

with raw_customers
    as (

        select
            *

        from {{ ref('scd_customers') }} --jaffle.raw_schema.raw_customers

    )

select

    id as customer_id,

    name
    as customer_name,

     DBT_SCD_ID, 
     DBT_UPDATED_AT, 
     DBT_VALID_FROM, 
     DBT_VALID_TO

     

from raw_customers



