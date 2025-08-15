

-- Top selling Product by revenue
select * from {{ ref('inter_products_enriched') }} where total_revenue = (
select max(total_revenue) from {{ ref('inter_products_enriched') }}
)

-- Top 3 selling product by NUMBER_OF_ITEMS_SOLD
with top_selling_product as (
select *, dense_rank() over(partition by product_type order by NUMBER_OF_ITEMS_SOLD desc) as rank 
from  {{ ref('inter_products_enriched') }}
)
select product_id, product_name, product_type, NUMBER_OF_ITEMS_SOLD, rank from top_selling_product where rank <= 3

