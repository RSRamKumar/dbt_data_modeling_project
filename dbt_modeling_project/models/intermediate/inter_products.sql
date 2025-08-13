
-- inter_products.sql   1 row = 1 product


select product_id, product_name, product_type, product_price from {{ ref('stg_products') }}