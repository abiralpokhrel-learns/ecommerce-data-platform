{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-01-01' as date)"
    ) }}
),

dates as (
    select
        date_day::date as date_day,
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        to_char(date_day, 'Month') as month_name,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Day') as day_name,
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from dates