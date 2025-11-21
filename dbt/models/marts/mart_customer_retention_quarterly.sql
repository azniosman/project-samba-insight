{{
    config(
        materialized='table',
        tags=['mart', 'customer', 'retention', 'executive'],
        partition_by={
            "field": "cohort_quarter_start",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=['customer_state', 'customer_city']
    )
}}

/*
    Quarterly Customer Retention Analysis

    Tracks quarterly repeat purchase rates by cohort for executive reporting.
    Answers: "What % of Q1 2017 customers purchased again in Q2? Q3? Q4?"

    Grain: One row per cohort_quarter, geographic location, quarters_since_cohort

    Business Impact: Retention improvement drives 80%+ of revenue growth
*/

with customer_first_order as (
    select
        c.customer_key,
        c.customer_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment,

        -- First order details
        min(f.order_purchase_date) as first_order_date,
        min(f.total_order_value) as first_order_value,

        -- Cohort assignment (quarterly)
        date_trunc(min(f.order_purchase_date), quarter) as cohort_quarter_start,

        -- Cohort labels for reporting
        format_date('%Y-Q%Q', date_trunc(min(f.order_purchase_date), quarter)) as cohort_quarter_label,
        extract(year from date_trunc(min(f.order_purchase_date), quarter)) as cohort_year,
        extract(quarter from date_trunc(min(f.order_purchase_date), quarter)) as cohort_quarter

    from {{ ref('dim_customer') }} as c
    inner join {{ ref('fact_orders') }} as f
        on c.customer_key = f.customer_key

    where f.order_status not in ('canceled', 'unavailable')

    group by
        c.customer_key,
        c.customer_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment
),

customer_quarterly_activity as (
    select
        cfo.cohort_quarter_start,
        cfo.cohort_quarter_label,
        cfo.customer_state,
        cfo.customer_city,
        cfo.customer_key,
        cfo.customer_segment,
        cfo.first_order_value,

        -- Order details
        f.order_id,
        f.order_purchase_date,
        date_trunc(f.order_purchase_date, quarter) as order_quarter_start,
        f.total_order_value,

        -- Calculate quarters since cohort (0 = acquisition quarter)
        date_diff(
            date_trunc(f.order_purchase_date, quarter),
            cfo.cohort_quarter_start,
            quarter
        ) as quarters_since_cohort

    from customer_first_order as cfo
    inner join {{ ref('fact_orders') }} as f
        on cfo.customer_key = f.customer_key

    where f.order_status not in ('canceled', 'unavailable')
),

-- Cohort size by geography
cohort_size as (
    select
        cohort_quarter_start,
        cohort_quarter_label,
        customer_state,
        customer_city,

        -- Cohort demographics
        count(distinct customer_key) as cohort_size,

        -- Segment breakdown
        sum(case when customer_segment = 'loyal' then 1 else 0 end) as loyal_customers_at_cohort,
        sum(case when customer_segment = 'repeat' then 1 else 0 end) as repeat_customers_at_cohort,
        sum(case when customer_segment = 'one_time' then 1 else 0 end) as one_time_customers_at_cohort,

        -- First order economics
        avg(first_order_value) as avg_first_order_value,
        sum(first_order_value) as total_first_order_revenue

    from customer_first_order
    group by 1, 2, 3, 4
),

-- Retention metrics by quarter
quarterly_retention as (
    select
        cohort_quarter_start,
        cohort_quarter_label,
        customer_state,
        customer_city,
        quarters_since_cohort,

        -- Retention metrics (REPEAT PURCHASE RATE)
        count(distinct customer_key) as active_customers,

        -- Order metrics
        count(distinct order_id) as total_orders,
        sum(total_order_value) as total_revenue,

        -- Per-customer metrics
        avg(total_order_value) as avg_order_value,
        count(distinct order_id) * 1.0 / nullif(count(distinct customer_key), 0) as avg_orders_per_customer

    from customer_quarterly_activity
    where quarters_since_cohort >= 0

    group by 1, 2, 3, 4, 5
),

final as (
    select
        -- Cohort identifiers
        qr.cohort_quarter_start,
        qr.cohort_quarter_label,
        extract(year from qr.cohort_quarter_start) as cohort_year,
        extract(quarter from qr.cohort_quarter_start) as cohort_quarter,

        -- Geography
        qr.customer_state,
        qr.customer_city,

        -- Time dimension
        qr.quarters_since_cohort,

        -- Cohort size and composition
        cs.cohort_size,
        cs.loyal_customers_at_cohort,
        cs.repeat_customers_at_cohort,
        cs.one_time_customers_at_cohort,

        -- Retention metrics
        qr.active_customers,
        qr.total_orders,
        qr.total_revenue,
        qr.avg_order_value,
        qr.avg_orders_per_customer,

        -- QUARTERLY REPEAT PURCHASE RATE
        -- Q0 = 100% (acquisition), Q1+ = % who purchased again
        round(
            qr.active_customers * 100.0 / nullif(cs.cohort_size, 0),
            2
        ) as retention_rate_pct,

        -- Cumulative metrics
        sum(qr.total_revenue) over (
            partition by qr.cohort_quarter_start, qr.customer_state, qr.customer_city
            order by qr.quarters_since_cohort
            rows between unbounded preceding and current row
        ) as cumulative_revenue,

        sum(qr.total_orders) over (
            partition by qr.cohort_quarter_start, qr.customer_state, qr.customer_city
            order by qr.quarters_since_cohort
            rows between unbounded preceding and current row
        ) as cumulative_orders,

        -- Customer Lifetime Value (to date)
        round(
            sum(qr.total_revenue) over (
                partition by qr.cohort_quarter_start, qr.customer_state, qr.customer_city
                order by qr.quarters_since_cohort
                rows between unbounded preceding and current row
            ) / nullif(cs.cohort_size, 0),
            2
        ) as ltv_to_date,

        -- First order economics
        cs.avg_first_order_value,
        cs.total_first_order_revenue,

        -- Metadata
        current_timestamp() as _updated_at

    from quarterly_retention as qr
    inner join cohort_size as cs
        on qr.cohort_quarter_start = cs.cohort_quarter_start
        and qr.customer_state = cs.customer_state
        and qr.customer_city = cs.customer_city
)

select * from final
