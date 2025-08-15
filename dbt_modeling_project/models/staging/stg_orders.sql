with raw_orders
    as (

        select
            *

        from {{ source('jaffle', 'orders') }} --jaffle.raw_schema.raw_orders

    )

select

    id as order_id,

    customer as customer_id,

    to_date(ordered_at)  as order_date,

    store_id,

    sub_total,

    tax_paid,

    order_total as order_total_amount,

    current_timestamp()
    as load_timestamp

from raw_orders

