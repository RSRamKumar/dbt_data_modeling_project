
with raw_stores
    as (

        select
            *

        from {{ source('jaffle', 'stores') }} --jaffle.raw_schema.raw_stores

    )

select

    id as store_id,

    name
    as location_name,

    opened_at as store_opened_time,

    current_timestamp()
    as load_timestamp

from raw_stores