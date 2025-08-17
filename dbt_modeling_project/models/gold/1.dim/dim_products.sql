
--  dim_products.sql


with product_info as (
        select product_id, product_name, product_type, product_price from {{ ref('stg_products')}}
        where is_current = true
    )

select
    {{ dbt_utils.generate_surrogate_key(['product_id' ]) }} as product_sk,
    product_id,
    product_name,
    product_type,
    product_price,

    current_timestamp() as load_timestamp 


from product_info