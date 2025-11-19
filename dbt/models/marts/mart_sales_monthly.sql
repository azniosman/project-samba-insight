{{
    config(
        materialized='table',
        tags=['mart', 'sales']
    )
}}

/*
    Monthly Sales Mart

    Aggregated monthly sales metrics for trend analysis and reporting.
    Optimized for executive dashboards and monthly business reviews.

    Grain: One row per month, product category, and customer state combination
*/

with monthly_sales as (
    select
        year,
        quarter,
        month as month_number,
        month_name,
        customer_state,

        -- Geography
        product_category,

        -- Product
        product_tier,
        date_trunc(order_date, month) as month_date,

        -- Aggregate daily metrics
        sum(total_orders) as total_orders,
        sum(unique_customers) as unique_customers,

        -- Revenue metrics
        sum(total_revenue) as total_revenue,
        avg(avg_order_value) as avg_order_value,
        min(min_order_value) as min_order_value,
        max(max_order_value) as max_order_value,

        -- Payment metrics
        sum(total_payments) as total_payments,
        avg(avg_installments) as avg_installments,
        sum(payment_mismatches) as payment_mismatches,

        -- Delivery metrics
        sum(delivered_orders) as delivered_orders,
        sum(on_time_deliveries) as on_time_deliveries,
        avg(avg_delivery_days) as avg_delivery_days,

        -- Review metrics
        avg(avg_review_score) as avg_review_score,
        sum(positive_reviews) as positive_reviews,
        sum(neutral_reviews) as neutral_reviews,
        sum(negative_reviews) as negative_reviews,

        -- Quality metrics
        sum(quality_issues) as quality_issues

    from {{ ref('mart_sales_daily') }}

    group by
        date_trunc(order_date, month),
        year,
        quarter,
        month,
        month_name,
        customer_state,
        product_category,
        product_tier
),

with_calculations as (
    select
        *,

        -- Calculated metrics
        round(total_revenue / nullif(total_orders, 0), 2) as revenue_per_order,
        round(total_revenue / nullif(unique_customers, 0), 2) as revenue_per_customer,
        round(delivered_orders * 100.0 / nullif(total_orders, 0), 2) as delivery_rate_pct,
        round(on_time_deliveries * 100.0 / nullif(delivered_orders, 0), 2) as on_time_rate_pct,
        round(positive_reviews * 100.0 / nullif(total_orders, 0), 2) as positive_review_rate_pct,

        -- Total days in month (for averaging)
        extract(day from last_day(month_date)) as days_in_month

    from monthly_sales
),

with_trends as (
    select
        *,

        -- Daily averages
        round(total_orders / days_in_month, 2) as avg_daily_orders,
        round(total_revenue / days_in_month, 2) as avg_daily_revenue,

        -- Month-over-month growth
        round(
            (total_revenue - lag(total_revenue) over (
                partition by customer_state, product_category
                order by month_date
            )) * 100.0 / nullif(lag(total_revenue) over (
                partition by customer_state, product_category
                order by month_date
            ), 0),
            2
        ) as mom_revenue_growth_pct,

        round(
            (total_orders - lag(total_orders) over (
                partition by customer_state, product_category
                order by month_date
            )) * 100.0 / nullif(lag(total_orders) over (
                partition by customer_state, product_category
                order by month_date
            ), 0),
            2
        ) as mom_order_growth_pct,

        -- Year-over-year growth
        round(
            (total_revenue - lag(total_revenue, 12) over (
                partition by customer_state, product_category
                order by month_date
            )) * 100.0 / nullif(lag(total_revenue, 12) over (
                partition by customer_state, product_category
                order by month_date
            ), 0),
            2
        ) as yoy_revenue_growth_pct,

        -- Timestamp
        current_timestamp() as _updated_at

    from with_calculations
)

select * from with_trends
