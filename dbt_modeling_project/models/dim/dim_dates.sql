

-- dim_dates.sql
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)

select
    date_day as date_actual,
    to_char(date_day, 'YYYYMMDD')    as date_id,
    extract(year from date_day)        as year,
    extract(month from date_day)       as month,
    extract(day from date_day)         as day,
    to_char(date_day, 'Month')         as month_name,
    to_char(date_day, 'Dy')            as day_name,
    extract(quarter from date_day)     as quarter,
    current_timestamp() as load_timestamp 
from date_spine
order by date_actual
