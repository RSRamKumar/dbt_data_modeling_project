

with raw_customers
    as (

        select
            *

        from  jaffle.raw_schema.raw_customers

    )

select

    id as customer_id,

    name
    as customer_name,

    current_timestamp()
    as load_timestamp

from raw_customers



