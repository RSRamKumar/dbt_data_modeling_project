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


{# 

-- dim_customers.sql
-- 1 row = 1 version of a customer (tracks SCD changes)

with base as (
    -- take the raw SCD-tracked customers
    select
        customer_id,
        customer_name,
        coalesce(dbt_valid_from::date, '1900-01-01'::date) as effective_date,
        coalesce(dbt_valid_to::date,   '2999-12-31'::date) as end_date,
        case when dbt_valid_to is null then true else false end as is_current
    from {{ ref('stg_customers') }}
),

orders as (
    -- bring in aggregated order metrics
    select
        customer_id,
        min(order_date)  as first_order_date,
        max(order_date)  as recent_order_date,
        count(*)         as lifetime_orders_count,
        sum(order_total_amount) as lifetime_total_amount
    from {{ ref('inter_customer_orders_enriched') }}
    group by customer_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.customer_id', 'b.customer_name', 'b.effective_date']) }} as customer_sk,
        b.customer_id,
        b.customer_name,
        o.first_order_date,
        o.recent_order_date,
        o.lifetime_orders_count,
        o.lifetime_total_amount,
        datediff('day', o.first_order_date, o.recent_order_date) as customer_active_span,

        b.effective_date,
        b.end_date,
        b.is_current,
        current_timestamp() as load_timestamp
    from base b
    left join orders o
      on b.customer_id = o.customer_id
)

select * from final where customer_id = '4ae72884-7442-4556-8beb-39b3cfe549d5' #}
