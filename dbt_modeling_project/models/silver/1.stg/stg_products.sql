with raw_products as (
    select
        sku,
        name,
        type,
        price,
        description,
        DBT_SCD_ID,
        DBT_UPDATED_AT,
        DBT_VALID_FROM,
        DBT_VALID_TO
    from {{ ref('scd_products') }}
)

select
    sku as product_id,
    name as product_name,
    type as product_type,
    {{ cents_to_euro('price') }} as product_price,
    description as product_description,
    DBT_SCD_ID,
    DBT_UPDATED_AT,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    case when DBT_VALID_TO is null then true else false end as is_current
from raw_products
