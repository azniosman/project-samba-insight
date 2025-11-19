{{
    config(
        materialized='table',
        tags=['mart', 'customer']
    )
}}

/*
    Customer Retention Analysis Mart

    Month-by-month retention analysis showing how many customers
    from each cohort return to make purchases in subsequent months.

    Grain: One row per cohort month, activity month, and customer state
*/

with customer_first_order as (
    select
        c.customer_key,
        c.customer_state,
        min(f.order_purchase_date) as first_order_date,
        date_trunc(min(f.order_purchase_date), month) as cohort_month

    from {{ ref('dim_customer') }} as c
    inner join {{ ref('fact_orders') }} as f
        on c.customer_key = f.customer_key

    where f.order_status != 'canceled'

    group by
        c.customer_key,
        c.customer_state
),

customer_monthly_activity as (
    select
        cfo.customer_key,
        cfo.customer_state,
        cfo.cohort_month,
        date_trunc(f.order_purchase_date, month) as activity_month,

        -- Metrics for this customer in this month
        count(distinct f.order_id) as orders,
        sum(f.total_order_value) as revenue,
        avg(f.review_score) as avg_review_score

    from customer_first_order as cfo
    inner join {{ ref('fact_orders') }} as f
        on
            cfo.customer_key = f.customer_key
            and f.order_status != 'canceled'

    group by
        cfo.customer_key,
        cfo.customer_state,
        cfo.cohort_month,
        date_trunc(f.order_purchase_date, month)
),

retention_metrics as (
    select
        cohort_month,
        activity_month,
        customer_state,
        format_date('%Y-%m', cohort_month) as cohort_month_name,
        format_date('%Y-%m', activity_month) as activity_month_name,

        -- Calculate months since cohort
        date_diff(activity_month, cohort_month, month) as months_since_cohort,

        -- Count customers
        count(distinct customer_key) as active_customers,

        -- Revenue metrics
        sum(revenue) as total_revenue,
        avg(revenue) as avg_revenue_per_customer,

        -- Order metrics
        sum(orders) as total_orders,
        avg(orders) as avg_orders_per_customer,

        -- Review score
        avg(avg_review_score) as avg_review_score

    from customer_monthly_activity

    group by
        cohort_month,
        format_date('%Y-%m', cohort_month),
        activity_month,
        format_date('%Y-%m', activity_month),
        customer_state,
        date_diff(activity_month, cohort_month, month)
),

with_cohort_size as (
    select
        r.*,

        -- Get cohort size (customers in month 0)
        first_value(active_customers) over (
            partition by cohort_month, customer_state
            order by months_since_cohort
        ) as cohort_size

    from retention_metrics as r
),

with_calculations as (
    select
        *,

        -- Retention rate (active customers / cohort size)
        round(active_customers * 100.0 / nullif(cohort_size, 0), 2) as retention_rate_pct,

        -- Month-over-month retention (active this month / active last month)
        round(
            active_customers * 100.0 / nullif(
                lag(active_customers) over (
                    partition by cohort_month, customer_state
                    order by months_since_cohort
                ),
                0
            ),
            2
        ) as mom_retention_pct,

        -- Cumulative metrics
        sum(total_revenue) over (
            partition by cohort_month, customer_state
            order by months_since_cohort
            rows between unbounded preceding and current row
        ) as cumulative_revenue,

        sum(total_orders) over (
            partition by cohort_month, customer_state
            order by months_since_cohort
            rows between unbounded preceding and current row
        ) as cumulative_orders,

        -- Timestamp
        current_timestamp() as _updated_at

    from with_cohort_size
)

select
    cohort_month,
    cohort_month_name,
    activity_month,
    activity_month_name,
    customer_state,
    months_since_cohort,
    cohort_size,
    active_customers,
    retention_rate_pct,
    mom_retention_pct,
    total_revenue,
    avg_revenue_per_customer,
    cumulative_revenue,
    total_orders,
    avg_orders_per_customer,
    cumulative_orders,
    avg_review_score,
    _updated_at

from with_calculations
order by cohort_month desc, customer_state asc, months_since_cohort asc
