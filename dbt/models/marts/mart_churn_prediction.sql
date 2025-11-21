{{
    config(
        materialized='table',
        tags=['mart', 'churn', 'executive', 'ml'],
        cluster_by=['churn_risk_segment', 'customer_state']
    )
}}

/*
    Customer Churn Prediction & Risk Scoring

    Rule-based churn prediction model using RFM + behavioral signals.
    Identifies at-risk customers for retention campaigns.

    Churn Definition: No purchase in last 90 days (adjustable via dbt var)

    Grain: One row per active customer

    Business Impact: Proactive retention reduces churn by 15-25%
*/

with customer_activity as (
    select
        c.customer_key,
        c.customer_id,
        c.customer_state,
        c.customer_city,
        c.customer_segment,

        -- Recency metrics
        max(f.order_purchase_date) as last_order_date,
        date_diff(current_date(), max(f.order_purchase_date), day) as days_since_last_order,

        min(f.order_purchase_date) as first_order_date,
        date_diff(current_date(), min(f.order_purchase_date), day) as customer_lifetime_days,

        -- Frequency metrics
        count(distinct f.order_id) as total_orders,
        count(distinct f.order_id) * 1.0 / nullif(
            date_diff(current_date(), min(f.order_purchase_date), day) / 30.0,
            0
        ) as avg_orders_per_month,

        -- Monetary metrics
        sum(f.total_order_value) as total_revenue,
        avg(f.total_order_value) as avg_order_value,

        -- Trend analysis (recent vs historical)
        sum(case
            when f.order_purchase_date >= date_sub(current_date(), interval 90 day)
            then f.total_order_value
            else 0
        end) as revenue_last_90_days,

        count(distinct case
            when f.order_purchase_date >= date_sub(current_date(), interval 90 day)
            then f.order_id
        end) as orders_last_90_days,

        sum(case
            when f.order_purchase_date >= date_sub(current_date(), interval 180 day)
                and f.order_purchase_date < date_sub(current_date(), interval 90 day)
            then f.total_order_value
            else 0
        end) as revenue_previous_90_days,

        count(distinct case
            when f.order_purchase_date >= date_sub(current_date(), interval 180 day)
                and f.order_purchase_date < date_sub(current_date(), interval 90 day)
            then f.order_id
        end) as orders_previous_90_days,

        -- Satisfaction signals
        avg(f.review_score) as avg_review_score,
        sum(case when f.review_score <= 2 then 1 else 0 end) as negative_reviews,
        sum(case when f.is_late_delivery then 1 else 0 end) as late_deliveries,
        sum(case when f.is_late_delivery then 1 else 0 end) * 100.0 / count(*) as late_delivery_rate_pct,

        -- Cancellation history
        sum(case when f.order_status = 'canceled' then 1 else 0 end) as canceled_orders,
        sum(case when f.order_status = 'canceled' then 1 else 0 end) * 100.0 / count(*) as cancellation_rate_pct

    from {{ ref('dim_customer') }} as c
    inner join {{ ref('fact_orders') }} as f
        on c.customer_key = f.customer_key

    group by 1, 2, 3, 4, 5
),

churn_signals as (
    select
        *,

        -- Signal 1: Recency (days since last order)
        case
            when days_since_last_order > 180 then 100  -- Churned
            when days_since_last_order > 120 then 80   -- High risk
            when days_since_last_order > 90 then 60    -- Medium risk
            when days_since_last_order > 60 then 40    -- Low risk
            when days_since_last_order > 30 then 20    -- Active
            else 0  -- Very active
        end as recency_risk_score,

        -- Signal 2: Declining engagement
        case
            when orders_last_90_days = 0 and orders_previous_90_days > 0 then 100  -- Stopped ordering
            when orders_last_90_days < orders_previous_90_days then 60             -- Declining
            when orders_last_90_days = orders_previous_90_days then 30            -- Stable
            else 0  -- Growing
        end as engagement_risk_score,

        -- Signal 3: Declining spend
        case
            when revenue_last_90_days = 0 and revenue_previous_90_days > 0 then 100
            when revenue_last_90_days < (revenue_previous_90_days * 0.5) then 70
            when revenue_last_90_days < revenue_previous_90_days then 40
            else 0
        end as spend_risk_score,

        -- Signal 4: Satisfaction issues
        case
            when avg_review_score < 2.5 then 80
            when avg_review_score < 3.5 then 50
            when avg_review_score < 4.0 then 20
            else 0
        end as satisfaction_risk_score,

        -- Signal 5: Service quality issues
        case
            when late_delivery_rate_pct > 50 then 70
            when late_delivery_rate_pct > 30 then 40
            when late_delivery_rate_pct > 15 then 20
            else 0
        end as service_risk_score,

        -- Signal 6: Cancellation behavior
        case
            when cancellation_rate_pct > 20 then 60
            when cancellation_rate_pct > 10 then 30
            else 0
        end as cancellation_risk_score

    from customer_activity
),

churn_prediction as (
    select
        *,

        -- Composite churn risk score (weighted average)
        round(
            (recency_risk_score * 0.35) +           -- Recency is most important
            (engagement_risk_score * 0.20) +
            (spend_risk_score * 0.20) +
            (satisfaction_risk_score * 0.15) +
            (service_risk_score * 0.05) +
            (cancellation_risk_score * 0.05),
            2
        ) as churn_risk_score,

        -- Churn probability (0-1)
        round(
            (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) / 100.0,
            3
        ) as churn_probability,

        -- Churn status
        case
            when days_since_last_order > 180 then 'Churned'
            when days_since_last_order > 90 then 'At Risk'
            when days_since_last_order > 60 then 'Declining'
            else 'Active'
        end as churn_status,

        -- Risk segmentation
        case
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 80 then 'Critical Risk'
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 60 then 'High Risk'
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 40 then 'Medium Risk'
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 20 then 'Low Risk'
            else 'Healthy'
        end as churn_risk_segment,

        -- Customer value (for prioritization)
        case
            when total_revenue >= 1000 and total_orders >= 5 then 'VIP'
            when total_revenue >= 500 and total_orders >= 3 then 'High Value'
            when total_revenue >= 200 then 'Medium Value'
            else 'Low Value'
        end as customer_value_tier,

        -- Retention priority (combine risk + value)
        case
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 60
                and total_revenue >= 500 then 'URGENT: High-value at risk'
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 60 then 'High: At risk customer'
            when (
                (recency_risk_score * 0.35) +
                (engagement_risk_score * 0.20) +
                (spend_risk_score * 0.20) +
                (satisfaction_risk_score * 0.15) +
                (service_risk_score * 0.05) +
                (cancellation_risk_score * 0.05)
            ) >= 40
                and total_revenue >= 300 then 'Medium: Monitor closely'
            else 'Low: Standard monitoring'
        end as retention_priority,

        -- Recommended action
        case
            when days_since_last_order > 180 then 'Win-back campaign'
            when days_since_last_order > 90 and negative_reviews > 0 then 'Satisfaction recovery'
            when days_since_last_order > 90 and avg_order_value < 100 then 'Incentive offer'
            when days_since_last_order > 60 and total_orders = 1 then 'Second purchase campaign'
            when days_since_last_order > 60 then 'Re-engagement email'
            else 'Standard nurture'
        end as recommended_action,

        current_timestamp() as _updated_at

    from churn_signals
)

select * from churn_prediction
