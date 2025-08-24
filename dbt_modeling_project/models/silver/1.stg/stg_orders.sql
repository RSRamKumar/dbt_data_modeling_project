with raw_orders
    as (

        select
            *

        from {{ source('jaffle', 'orders') }} --jaffle.raw_schema.raw_orders

    )

select

    id as order_id,

    customer as customer_id,

    to_date(ordered_at) as order_date,

    store_id,

    {{ cents_to_euro('sub_total') }} as sub_total,

    {{ cents_to_euro('tax_paid') }} as tax_paid,

    {{ cents_to_euro('order_total') }} as order_total_amount

from raw_orders

