with raw_products 
as (

    select 
* 

    from jaffle.raw_schema.raw_products

)

select 

    sku as product_id,

    name 
as product_name,

    type 
as product_type,

    price as product_price,

    description 
as product_description,

    current_timestamp() 
as load_timestamp

from raw_products