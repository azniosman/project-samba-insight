{{
    config(
        materialized='table',
        tags=['dimension', 'date']
    )
}}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2016-09-04' as date)",
            end_date="cast('2018-10-17' as date)"
        )
    }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,

        -- Natural Key
        date_day,

        -- Date Parts
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        extract(day from date_day) as day_of_month,

        -- Formatted Dates
        format_date('%B', date_day) as month_name,
        format_date('%b', date_day) as month_name_short,
        format_date('%A', date_day) as day_name,
        format_date('%a', date_day) as day_name_short,
        format_date('%Y-%m', date_day) as year_month,
        format_date('%Y-Q%Q', date_day) as year_quarter,

        -- Flags
        case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) between 2 and 6 then true else false end as is_weekday,
        case when extract(day from date_day) = 1 then true else false end as is_month_start,
        case
            when extract(day from date_day) = extract(day from last_day(date_day))
            then true
            else false
        end as is_month_end,

        -- Relative Dates
        date_add(date_day, interval -1 day) as previous_day,
        date_add(date_day, interval 1 day) as next_day,
        date_add(date_day, interval -7 day) as same_day_last_week,
        date_add(date_day, interval -1 month) as same_day_last_month,
        date_add(date_day, interval -1 year) as same_day_last_year

    from date_spine
)

select * from final
