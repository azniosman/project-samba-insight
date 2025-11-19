{{
    config(
        materialized='table',
        tags=['mart', 'customer']
    )
}}

/*
    Customer Cohort Analysis Mart

    Cohort analysis based on customer acquisition month.
    Tracks customer behavior and value over time by cohort.

    Grain: One row per cohort month and customer state
*/

with customer_first_order as (
    select
        c.customer_key,
        c.customer_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment,
        min(f.order_purchase_date) as first_order_date,
        date_trunc(min(f.order_purchase_date), month) as cohort_month

    from {{ ref('dim_customer') }} as c
    inner join {{ ref('fact_orders') }} as f
        on c.customer_key = f.customer_key

    where f.order_status != 'canceled'

    group by
        c.customer_key,
        c.customer_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment
),

cohort_metrics as (
    select
        cfo.cohort_month,
        cfo.customer_state,
        extract(year from cfo.cohort_month) as cohort_year,
        extract(month from cfo.cohort_month) as cohort_month_number,
        format_date('%Y-%m', cfo.cohort_month) as cohort_month_name,

        -- Cohort size
        count(distinct cfo.customer_key) as cohort_size,

        -- Segment distribution
        sum(case when cfo.customer_segment = 'loyal' then 1 else 0 end) as loyal_customers,
        sum(case when cfo.customer_segment = 'repeat' then 1 else 0 end) as repeat_customers,
        sum(case when cfo.customer_segment = 'one_time' then 1 else 0 end) as one_time_customers,

        -- Lifetime metrics (all orders from cohort customers)
        count(distinct f.order_id) as total_orders,
        sum(f.total_order_value) as total_revenue,
        avg(f.total_order_value) as avg_order_value,

        -- First month metrics
        sum(case
            when date_trunc(f.order_purchase_date, month) = cfo.cohort_month
                then f.total_order_value
            else 0
        end) as first_month_revenue,

        count(distinct case
            when date_trunc(f.order_purchase_date, month) = cfo.cohort_month
                then f.order_id
        end) as first_month_orders,

        -- Review metrics
        avg(f.review_score) as avg_review_score,
        sum(case when f.review_sentiment = 'positive' then 1 else 0 end) as positive_reviews,

        -- Delivery metrics
        avg(f.delivery_days) as avg_delivery_days,
        sum(case when f.is_on_time_delivery then 1 else 0 end) as on_time_deliveries,
        sum(case when f.is_delivered then 1 else 0 end) as delivered_orders

    from customer_first_order as cfo
    inner join {{ ref('fact_orders') }} as f
        on
            cfo.customer_key = f.customer_key
            and f.order_status != 'canceled'

    group by
        cfo.cohort_month,
        extract(year from cfo.cohort_month),
        extract(month from cfo.cohort_month),
        format_date('%Y-%m', cfo.cohort_month),
        cfo.customer_state
),

with_calculations as (
    select
        *,

        -- Customer lifetime value
        round(total_revenue / nullif(cohort_size, 0), 2) as customer_ltv,

        -- Average orders per customer
        round(total_orders * 1.0 / nullif(cohort_size, 0), 2) as avg_orders_per_customer,

        -- First month metrics
        round(first_month_revenue / nullif(cohort_size, 0), 2) as revenue_per_customer_first_month,
        round(first_month_orders * 1.0 / nullif(cohort_size, 0), 2) as orders_per_customer_first_month,

        -- Segment percentages
        round(loyal_customers * 100.0 / nullif(cohort_size, 0), 2) as loyal_pct,
        round(repeat_customers * 100.0 / nullif(cohort_size, 0), 2) as repeat_pct,
        round(one_time_customers * 100.0 / nullif(cohort_size, 0), 2) as one_time_pct,

        -- Quality metrics
        round(on_time_deliveries * 100.0 / nullif(delivered_orders, 0), 2) as on_time_delivery_pct,
        round(positive_reviews * 100.0 / nullif(total_orders, 0), 2) as positive_review_pct,

        -- Timestamp
        current_timestamp() as _updated_at

    from cohort_metrics
)

select * from with_calculations
order by cohort_month desc, customer_state asc
