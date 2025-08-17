with raw_order_items
    as (

        select
            *

        from {{ source('jaffle', 'order_items') }} -- jaffle.raw_schema.raw_order_items

    )

select

    id as order_item_id,

    order_id,

    sku as product_id

from raw_order_items