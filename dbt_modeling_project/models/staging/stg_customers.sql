

with raw_customers
    as (

        select
            *

        from {{ ref('scd_customers') }}

    )

select

    id as customer_id,

    name
    as customer_name,

    DBT_SCD_ID,
    DBT_UPDATED_AT,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    case when DBT_VALID_TO is null then true else false end as is_current




from raw_customers



