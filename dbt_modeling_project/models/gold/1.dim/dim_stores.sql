
-- dim_stores.sql

select
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_sk,
    store_id,
    location_name,
    current_timestamp() as load_timestamp

from {{ ref('stg_store_locations') }}






