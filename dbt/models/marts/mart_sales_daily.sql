{{
    config(
        materialized='table',
        tags=['mart', 'sales'],
        partition_by={
            "field": "order_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['product_category', 'customer_state']
    )
}}

/*
    Daily Sales Mart

    Business-focused aggregation of daily sales metrics for analytics and dashboards.
    Pre-aggregated data improves dashboard query performance.

    Grain: One row per day, product category, and customer state combination
*/

with daily_sales as (
    select
        f.order_purchase_date as order_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,

        -- Geography
        c.customer_state,
        c.customer_city,

        -- Product
        p.product_category_name_en as product_category,
        p.sales_tier as product_tier,

        -- Order metrics
        count(distinct f.order_id) as total_orders,
        count(distinct f.customer_key) as unique_customers,

        -- Revenue metrics
        sum(f.total_order_value) as total_revenue,
        avg(f.total_order_value) as avg_order_value,
        min(f.total_order_value) as min_order_value,
        max(f.total_order_value) as max_order_value,

        -- Payment metrics
        sum(f.total_payment_value) as total_payments,
        avg(f.max_installments) as avg_installments,
        sum(case when f.has_payment_mismatch then 1 else 0 end) as payment_mismatches,

        -- Delivery metrics
        sum(case when f.is_delivered then 1 else 0 end) as delivered_orders,
        sum(case when f.is_on_time_delivery then 1 else 0 end) as on_time_deliveries,
        avg(f.delivery_days) as avg_delivery_days,

        -- Review metrics
        avg(f.review_score) as avg_review_score,
        sum(case when f.review_sentiment = 'positive' then 1 else 0 end) as positive_reviews,
        sum(case when f.review_sentiment = 'neutral' then 1 else 0 end) as neutral_reviews,
        sum(case when f.review_sentiment = 'negative' then 1 else 0 end) as negative_reviews,

        -- Quality metrics
        sum(case when f.has_data_quality_issue then 1 else 0 end) as quality_issues

    from {{ ref('fact_orders') }} as f
    inner join {{ ref('dim_date') }} as d
        on f.order_purchase_date = d.date_day
    inner join {{ ref('dim_customer') }} as c
        on f.customer_key = c.customer_key
    left join {{ ref('stg_order_items') }} as oi
        on f.order_id = oi.order_id
    left join {{ ref('dim_product') }} as p
        on oi.product_id = p.product_id

    where f.order_status != 'canceled'

    group by
        f.order_purchase_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        c.customer_state,
        c.customer_city,
        p.product_category_name_en,
        p.sales_tier
),

final as (
    select
        *,

        -- Calculated metrics
        round(total_revenue / nullif(total_orders, 0), 2) as revenue_per_order,
        round(least(delivered_orders * 100.0 / nullif(total_orders, 0), 100), 2) as delivery_rate_pct,
        round(least(on_time_deliveries * 100.0 / nullif(delivered_orders, 0), 100), 2) as on_time_rate_pct,
        round(positive_reviews * 100.0 / nullif(total_orders, 0), 2) as positive_review_rate_pct,
        round(payment_mismatches * 100.0 / nullif(total_orders, 0), 2) as payment_mismatch_rate_pct,

        -- Data quality
        round(quality_issues * 100.0 / nullif(total_orders, 0), 2) as quality_issue_rate_pct,

        -- Timestamp
        current_timestamp() as _updated_at

    from daily_sales
)

select * from final
