{{
    config(
        materialized='table',
        tags=['mart', 'category', 'executive']
    )
}}

/*
    Product Category Performance Analysis

    Analyzes category-level metrics for diversification strategy.
    Identifies high-performing and underperforming categories.

    Grain: One row per product category

    Business Impact: Category diversification builds long-term stability
*/

with category_orders as (
    select
        p.product_category_name_en as category,
        p.product_category_name_pt as category_pt,

        -- Volume metrics
        count(distinct oi.order_id) as total_orders,
        count(distinct p.product_id) as unique_products,
        sum(oi.price) as total_revenue,
        sum(oi.freight_value) as total_freight,

        -- Average metrics
        avg(oi.price) as avg_product_price,
        avg(oi.freight_value) as avg_freight_per_item,

        -- Customer metrics
        count(distinct o.customer_id) as unique_customers,
        count(distinct oi.seller_id) as unique_sellers

    from {{ ref('stg_order_items') }} as oi
    inner join {{ ref('stg_products') }} as p
        on oi.product_id = p.product_id
    inner join {{ ref('stg_orders') }} as o
        on oi.order_id = o.order_id

    where o.order_status not in ('canceled', 'unavailable')
        and p.product_category_name_en is not null

    group by 1, 2
),

category_reviews as (
    select
        p.product_category_name_en as category,

        avg(r.review_score) as avg_review_score,
        count(distinct r.review_id) as total_reviews,
        sum(case when r.review_score >= 4 then 1 else 0 end) * 100.0 / count(*) as positive_review_pct,
        sum(case when r.review_score <= 2 then 1 else 0 end) * 100.0 / count(*) as negative_review_pct

    from {{ ref('stg_reviews') }} as r
    inner join {{ ref('stg_order_items') }} as oi
        on r.order_id = oi.order_id
    inner join {{ ref('stg_products') }} as p
        on oi.product_id = p.product_id

    where p.product_category_name_en is not null

    group by 1
),

category_performance as (
    select
        co.*,
        cr.avg_review_score,
        cr.total_reviews,
        cr.positive_review_pct,
        cr.negative_review_pct,

        -- Market share
        round(
            co.total_revenue * 100.0 / sum(co.total_revenue) over (),
            2
        ) as revenue_share_pct,

        round(
            co.total_orders * 100.0 / sum(co.total_orders) over (),
            2
        ) as order_share_pct,

        -- Customer metrics
        round(
            co.total_orders * 1.0 / nullif(co.unique_customers, 0),
            2
        ) as orders_per_customer,

        round(
            co.total_revenue / nullif(co.unique_customers, 0),
            2
        ) as revenue_per_customer,

        -- Supplier diversity
        round(
            co.unique_products * 1.0 / nullif(co.unique_sellers, 0),
            2
        ) as products_per_seller

    from category_orders as co
    left join category_reviews as cr
        on co.category = cr.category
),

with_rankings as (
    select
        *,

        -- Revenue ranking
        row_number() over (order by total_revenue desc) as revenue_rank,

        -- Order volume ranking
        row_number() over (order by total_orders desc) as order_rank,

        -- Customer satisfaction ranking
        row_number() over (order by avg_review_score desc) as satisfaction_rank,

        -- Strategic classification
        case
            when revenue_share_pct >= 10 then 'Core Category'
            when revenue_share_pct >= 5 then 'Growth Category'
            when revenue_share_pct >= 2 then 'Emerging Category'
            else 'Niche Category'
        end as strategic_tier,

        -- Performance assessment
        case
            when avg_review_score >= 4.0 and revenue_share_pct >= 5 then 'Star Performer'
            when avg_review_score >= 3.8 and revenue_share_pct >= 2 then 'Solid Performer'
            when avg_review_score < 3.5 or negative_review_pct > 20 then 'Needs Improvement'
            when revenue_share_pct < 1 then 'Under Development'
            else 'Stable'
        end as performance_status,

        -- Growth opportunity
        case
            when unique_customers > 1000 and orders_per_customer < 1.5 then 'High Repeat Potential'
            when avg_product_price < 50 and revenue_per_customer < 100 then 'Upsell Opportunity'
            when unique_sellers < 5 and total_orders > 500 then 'Expand Seller Base'
            when positive_review_pct > 80 and revenue_share_pct < 3 then 'Marketing Opportunity'
            else 'Optimize Operations'
        end as opportunity,

        -- Risk flags
        case when unique_sellers < 3 then true else false end as supplier_concentration_risk,
        case when negative_review_pct > 15 then true else false end as quality_risk,
        case when avg_freight_per_item > avg_product_price * 0.3 then true else false end as logistics_risk,

        current_timestamp() as _updated_at

    from category_performance
)

select * from with_rankings
order by total_revenue desc
