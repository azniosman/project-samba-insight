{{
    config(
        materialized='table',
        tags=['mart', 'geographic', 'executive'],
        cluster_by=['customer_state', 'customer_city']
    )
}}

/*
    Geographic Performance Analysis

    Deep-dive into state and city-level performance for expansion strategy.
    Identifies underserved markets and concentration risks.

    Grain: One row per state/city combination

    Business Impact: Geographic expansion reduces risk, taps new markets
*/

with customer_geography as (
    select
        customer_state,
        customer_city,
        count(distinct customer_key) as total_customers,
        count(distinct case when customer_segment = 'loyal' then customer_key end) as loyal_customers,
        count(distinct case when customer_segment = 'repeat' then customer_key end) as repeat_customers,
        count(distinct case when customer_segment = 'one_time' then customer_key end) as one_time_customers

    from {{ ref('dim_customer') }}
    where customer_state is not null
    group by 1, 2
),

order_geography as (
    select
        c.customer_state,
        c.customer_city,

        -- Order metrics
        count(distinct f.order_id) as total_orders,
        sum(f.total_order_value) as total_revenue,
        avg(f.total_order_value) as avg_order_value,

        -- Product metrics
        sum(f.total_products) as total_products_sold,
        avg(f.total_products) as avg_products_per_order,

        -- Delivery metrics
        avg(f.delivery_days) as avg_delivery_days,
        sum(case when f.is_on_time_delivery then 1 else 0 end) * 100.0 / count(*) as on_time_delivery_pct,
        sum(case when f.is_late_delivery then 1 else 0 end) * 100.0 / count(*) as late_delivery_pct,

        -- Review metrics
        avg(f.review_score) as avg_review_score,
        count(distinct case when f.review_score >= 4 then f.order_id end) * 100.0 / count(*) as positive_review_pct,

        -- Payment metrics
        avg(f.payment_methods_count) as avg_payment_methods,
        avg(f.max_installments) as avg_installments,

        -- Status breakdown
        sum(case when f.order_status = 'delivered' then 1 else 0 end) as delivered_orders,
        sum(case when f.order_status = 'canceled' then 1 else 0 end) as canceled_orders,
        sum(case when f.order_status = 'canceled' then 1 else 0 end) * 100.0 / count(*) as cancellation_rate_pct

    from {{ ref('dim_customer') }} as c
    inner join {{ ref('fact_orders') }} as f
        on c.customer_key = f.customer_key

    where c.customer_state is not null

    group by 1, 2
),

-- Calculate market share and concentration
market_metrics as (
    select
        og.*,
        cg.total_customers,
        cg.loyal_customers,
        cg.repeat_customers,
        cg.one_time_customers,

        -- Market share calculations
        round(
            og.total_revenue * 100.0 / sum(og.total_revenue) over (),
            2
        ) as revenue_share_pct,

        round(
            og.total_orders * 100.0 / sum(og.total_orders) over (),
            2
        ) as order_share_pct,

        round(
            cg.total_customers * 100.0 / sum(cg.total_customers) over (),
            2
        ) as customer_share_pct,

        -- State-level share (for concentration analysis)
        round(
            og.total_revenue * 100.0 / sum(og.total_revenue) over (partition by og.customer_state),
            2
        ) as city_share_of_state_revenue_pct,

        -- Customer retention rate
        round(
            (cg.loyal_customers + cg.repeat_customers) * 100.0 / nullif(cg.total_customers, 0),
            2
        ) as retention_rate_pct,

        -- Revenue per customer
        round(
            og.total_revenue / nullif(cg.total_customers, 0),
            2
        ) as revenue_per_customer,

        -- Orders per customer
        round(
            og.total_orders * 1.0 / nullif(cg.total_customers, 0),
            2
        ) as orders_per_customer

    from order_geography as og
    inner join customer_geography as cg
        on og.customer_state = cg.customer_state
        and og.customer_city = cg.customer_city
),

-- Rank cities for prioritization
with_rankings as (
    select
        *,

        -- Revenue ranking (within state)
        row_number() over (partition by customer_state order by total_revenue desc) as city_revenue_rank_in_state,

        -- Customer ranking (within state)
        row_number() over (partition by customer_state order by total_customers desc) as city_customer_rank_in_state,

        -- Overall revenue ranking
        row_number() over (order by total_revenue desc) as city_revenue_rank_overall,

        -- Growth potential score (high customers, low revenue = untapped)
        case
            when revenue_per_customer < 100 and total_customers > 50 then 'High Potential'
            when revenue_per_customer >= 200 and total_customers > 100 then 'Established'
            when revenue_per_customer >= 150 and total_customers < 50 then 'Premium Niche'
            when revenue_per_customer < 100 and total_customers < 50 then 'Developing'
            else 'Mature'
        end as market_maturity,

        -- Expansion priority
        case
            when retention_rate_pct > 50 and on_time_delivery_pct > 80 and avg_review_score > 4.0 then 'Expand Aggressively'
            when retention_rate_pct > 30 and on_time_delivery_pct > 70 then 'Expand Cautiously'
            when retention_rate_pct < 30 or on_time_delivery_pct < 60 then 'Fix Operations First'
            else 'Monitor'
        end as expansion_priority,

        -- Risk flags
        case when cancellation_rate_pct > 5 then true else false end as high_cancellation_risk,
        case when late_delivery_pct > 20 then true else false end as delivery_risk,
        case when avg_review_score < 3.5 then true else false end as satisfaction_risk,

        current_timestamp() as _updated_at

    from market_metrics
)

select * from with_rankings
