{{
    config(
        materialized='table',
        tags=['mart', 'executive', 'kpi']
    )
}}

/*
    Executive Summary KPIs

    Single source of truth for C-suite dashboard.
    Consolidates all critical metrics in one queryable table.

    Grain: One row per time period (quarterly)

    Business Impact: Executive visibility drives strategic decisions
*/

with quarterly_periods as (
    select distinct
        date_trunc(order_purchase_date, quarter) as quarter_start,
        format_date('%Y-Q%Q', date_trunc(order_purchase_date, quarter)) as quarter_label,
        extract(year from date_trunc(order_purchase_date, quarter)) as year,
        extract(quarter from date_trunc(order_purchase_date, quarter)) as quarter

    from {{ ref('fact_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

quarterly_revenue as (
    select
        date_trunc(order_purchase_date, quarter) as quarter_start,

        -- Revenue metrics
        sum(total_order_value) as total_revenue,
        avg(total_order_value) as avg_order_value,
        count(distinct order_id) as total_orders,
        count(distinct customer_key) as unique_customers,

        -- Revenue per customer
        sum(total_order_value) / nullif(count(distinct customer_key), 0) as revenue_per_customer,

        -- Product metrics
        sum(total_products) as total_products_sold,
        avg(total_products) as avg_products_per_order

    from {{ ref('fact_orders') }}
    where order_status not in ('canceled', 'unavailable')
    group by 1
),

quarterly_retention as (
    select
        cohort_quarter_start as quarter_start,

        -- Overall retention (Q1 repeat rate)
        avg(case when quarters_since_cohort = 1 then retention_rate_pct end) as q1_retention_rate,

        -- Cohort size
        sum(case when quarters_since_cohort = 0 then cohort_size end) as new_customers_acquired,

        -- LTV metrics
        avg(case when quarters_since_cohort = 4 then ltv_to_date end) as avg_ltv_after_4_quarters

    from {{ ref('mart_customer_retention_quarterly') }}
    group by 1
),

quarterly_churn as (
    select
        date_trunc(last_order_date, quarter) as quarter_start,

        -- Churn metrics
        count(distinct case when churn_status = 'Churned' then customer_key end) as churned_customers,
        count(distinct case when churn_risk_segment = 'Critical Risk' then customer_key end) as critical_risk_customers,
        count(distinct case when churn_risk_segment = 'High Risk' then customer_key end) as high_risk_customers,

        -- Average churn score
        avg(churn_risk_score) as avg_churn_risk_score

    from {{ ref('mart_churn_prediction') }}
    group by 1
),

quarterly_geography as (
    select
        quarter_start,

        -- Geographic diversity
        count(distinct customer_state) as active_states,
        count(distinct customer_city) as active_cities,

        -- Top state revenue
        max(case
            when row_num = 1 then revenue
        end) as top_state_revenue,

        -- Market concentration (top state %)
        max(case
            when row_num = 1 then revenue
        end) * 100.0 / sum(revenue) as top_state_concentration_pct

    from (
        select
            quarter_start,
            customer_state,
            customer_city,
            revenue,
            row_number() over (
                partition by quarter_start
                order by revenue desc
            ) as row_num
        from (
            select
                date_trunc(f.order_purchase_date, quarter) as quarter_start,
                c.customer_state,
                c.customer_city,
                sum(f.total_order_value) as revenue
            from {{ ref('fact_orders') }} as f
            inner join {{ ref('dim_customer') }} as c
                on f.customer_key = c.customer_key
            where f.order_status not in ('canceled', 'unavailable')
            group by date_trunc(f.order_purchase_date, quarter), c.customer_state, c.customer_city
        ) as grouped_revenue
    ) as state_revenue
    group by 1
),

quarterly_categories as (
    select
        quarter_start,

        -- Category diversity
        count(distinct product_category_name_en) as active_categories,

        -- Top category concentration
        max(case when row_num = 1 then category_revenue end) * 100.0 / sum(category_revenue) as top_category_concentration_pct

    from (
        select
            quarter_start,
            product_category_name_en,
            category_revenue,
            row_number() over (
                partition by quarter_start
                order by category_revenue desc
            ) as row_num
        from (
            select
                date_trunc(o.order_purchase_date, quarter) as quarter_start,
                p.product_category_name_en,
                sum(o.total_order_value) as category_revenue
            from {{ ref('fact_orders') }} as o
            inner join {{ ref('stg_order_items') }} as oi
                on o.order_id = oi.order_id
            inner join {{ ref('stg_products') }} as p
                on oi.product_id = p.product_id
            where o.order_status not in ('canceled', 'unavailable')
            group by date_trunc(o.order_purchase_date, quarter), p.product_category_name_en
        ) as grouped_category
    ) as ranked_categories
    group by 1
),

quarterly_satisfaction as (
    select
        date_trunc(order_purchase_date, quarter) as quarter_start,

        -- Satisfaction metrics
        avg(review_score) as avg_review_score,
        sum(case when review_score >= 4 then 1 else 0 end) * 100.0 / count(*) as positive_review_pct,

        -- Delivery performance
        avg(delivery_days) as avg_delivery_days,
        sum(case when is_on_time_delivery then 1 else 0 end) * 100.0 / count(*) as on_time_delivery_pct,

        -- Cancellation rate
        sum(case when order_status = 'canceled' then 1 else 0 end) * 100.0 / count(*) as cancellation_rate_pct

    from {{ ref('fact_orders') }}
    group by 1
),

executive_summary as (
    select
        qp.quarter_start,
        qp.quarter_label,
        qp.year,
        qp.quarter,

        -- Revenue & Growth
        qr.total_revenue,
        qr.avg_order_value,
        qr.total_orders,
        qr.unique_customers,
        qr.revenue_per_customer,
        qr.total_products_sold,
        qr.avg_products_per_order,

        -- Growth rates (QoQ)
        round(
            (qr.total_revenue - lag(qr.total_revenue) over (order by qp.quarter_start))
            * 100.0 / nullif(lag(qr.total_revenue) over (order by qp.quarter_start), 0),
            2
        ) as revenue_growth_qoq_pct,

        round(
            (qr.unique_customers - lag(qr.unique_customers) over (order by qp.quarter_start))
            * 100.0 / nullif(lag(qr.unique_customers) over (order by qp.quarter_start), 0),
            2
        ) as customer_growth_qoq_pct,

        -- Retention & LTV
        qret.q1_retention_rate as quarterly_retention_rate_pct,
        qret.new_customers_acquired,
        qret.avg_ltv_after_4_quarters,

        -- Churn Risk
        qc.churned_customers,
        qc.critical_risk_customers,
        qc.high_risk_customers,
        qc.avg_churn_risk_score,

        -- Geographic Metrics
        qg.active_states,
        qg.active_cities,
        round(qg.top_state_concentration_pct, 2) as geographic_concentration_pct,

        -- Category Metrics
        qcat.active_categories,
        round(qcat.top_category_concentration_pct, 2) as category_concentration_pct,

        -- Satisfaction & Operations
        round(qs.avg_review_score, 2) as avg_review_score,
        round(qs.positive_review_pct, 2) as positive_review_pct,
        round(qs.avg_delivery_days, 1) as avg_delivery_days,
        round(qs.on_time_delivery_pct, 2) as on_time_delivery_pct,
        round(qs.cancellation_rate_pct, 2) as cancellation_rate_pct,

        -- Strategic Health Score (0-100)
        round(
            (
                -- Revenue growth (20 points)
                case
                    when (qr.total_revenue - lag(qr.total_revenue) over (order by qp.quarter_start))
                        * 100.0 / nullif(lag(qr.total_revenue) over (order by qp.quarter_start), 0) >= 20
                    then 20
                    when (qr.total_revenue - lag(qr.total_revenue) over (order by qp.quarter_start))
                        * 100.0 / nullif(lag(qr.total_revenue) over (order by qp.quarter_start), 0) >= 10
                    then 15
                    when (qr.total_revenue - lag(qr.total_revenue) over (order by qp.quarter_start))
                        * 100.0 / nullif(lag(qr.total_revenue) over (order by qp.quarter_start), 0) >= 5
                    then 10
                    else 5
                end +

                -- Retention (25 points)
                case
                    when qret.q1_retention_rate >= 50 then 25
                    when qret.q1_retention_rate >= 40 then 20
                    when qret.q1_retention_rate >= 30 then 15
                    else 10
                end +

                -- Customer satisfaction (20 points)
                case
                    when qs.avg_review_score >= 4.5 then 20
                    when qs.avg_review_score >= 4.0 then 15
                    when qs.avg_review_score >= 3.5 then 10
                    else 5
                end +

                -- Geographic diversity (15 points)
                case
                    when qg.top_state_concentration_pct < 30 then 15
                    when qg.top_state_concentration_pct < 40 then 12
                    when qg.top_state_concentration_pct < 50 then 8
                    else 5
                end +

                -- Category diversity (10 points)
                case
                    when qcat.top_category_concentration_pct < 20 then 10
                    when qcat.top_category_concentration_pct < 30 then 7
                    else 5
                end +

                -- Operational excellence (10 points)
                case
                    when qs.on_time_delivery_pct >= 90 then 10
                    when qs.on_time_delivery_pct >= 80 then 7
                    when qs.on_time_delivery_pct >= 70 then 5
                    else 3
                end
            ),
            0
        ) as strategic_health_score,

        current_timestamp() as _updated_at

    from quarterly_periods as qp
    left join quarterly_revenue as qr
        on qp.quarter_start = qr.quarter_start
    left join quarterly_retention as qret
        on qp.quarter_start = qret.quarter_start
    left join quarterly_churn as qc
        on qp.quarter_start = qc.quarter_start
    left join quarterly_geography as qg
        on qp.quarter_start = qg.quarter_start
    left join quarterly_categories as qcat
        on qp.quarter_start = qcat.quarter_start
    left join quarterly_satisfaction as qs
        on qp.quarter_start = qs.quarter_start
)

select * from executive_summary
