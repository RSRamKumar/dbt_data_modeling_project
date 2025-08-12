with raw_supplies
    as (

        select
            *

        from {{ source('jaffle', 'supplies') }} --jaffle.raw_schema.raw_supplies

    )

select

    id as supply_id,

    name
    as supply_name,

    cost as supply_cost,

    perishable as is_product_perishable,

    sku as product_id,

    current_timestamp()
    as load_timestamp

from raw_supplies