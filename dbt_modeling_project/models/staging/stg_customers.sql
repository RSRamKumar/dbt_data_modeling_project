

with raw_customers
    as (

        select
            *

        from {{ ref('scd_customers') }} -- jaffle.dev_schema.scd_customers

    )

select

    id as customer_id,

    name
    as customer_name,

    lower(
        split_part(name, ' ', 1) | | '.' | | split_part(name, ' ', 2)
    ) | | '@email.com' as customer_email,

    DBT_SCD_ID,
    DBT_UPDATED_AT,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    case when DBT_VALID_TO is null then true else false end as IS_CURRENT




from raw_customers



