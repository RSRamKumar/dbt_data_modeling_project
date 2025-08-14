
-- Former inter_products.sql and now dim_products.sql


with product_info as (
    select product_id, product_name, product_type, product_price from {{ ref('stg_products')}}
)

select 
    dbt_utils.surrogate_key([product_id, product_name, product_type, product_price ]) as product_sk,
    product_id,
    product_name,
    product_type, 
    product_price,
    
    current_date as effective_date,
    null::date as end_date,
    true as is_current,
    current_timestamp() as load_timestamp

from product_info 