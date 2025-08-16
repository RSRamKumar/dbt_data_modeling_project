
-- Former inter_products.sql and now dim_products.sql


with product_info as (
    select product_id, product_name, product_type, product_price from {{ ref('stg_products')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['product_id', 'product_name', 'product_type', 'product_price' ]) }} as product_sk,
    product_id,
    product_name,
    product_type, 
    product_price,
    
    to_date('01-12-2010', 'DD-MM-YYYY') as effective_date,
    null::date as end_date,
    true as is_current,
     

from product_info 