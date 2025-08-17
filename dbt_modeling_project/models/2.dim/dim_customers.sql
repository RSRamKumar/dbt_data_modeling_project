-- dim_customers.sql
-- 1 row = 1 customer (current descriptive attributes only)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    customer_id,
    customer_name,
    customer_email,
    current_timestamp() as load_timestamp
from {{ ref('stg_customers') }}
where is_current = true


-- effective_date, end_date, is_current

 