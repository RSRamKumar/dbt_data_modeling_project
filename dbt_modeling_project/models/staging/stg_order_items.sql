with raw_order_items 
as (

    select 
* 

    from jaffle.raw_schema.raw_order_items

)

select 

    id as order_item_id,

    order_id,

    sku as product_id,

    current_timestamp() 
as load_timestamp

from raw_order_items