{{
    config(
        materialized='table',
        tags=['mart', 'product']
    )
}}

/*
    Product Performance Mart

    Comprehensive product and category performance metrics.
    Optimized for product analysis dashboards and inventory planning.

    Grain: One row per product and month
*/

with product_monthly_sales as (
    select
        p.product_id,
        p.product_category_name_en as product_category,
        p.product_category_name_pt,
        p.sales_tier,

        -- Product details
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        s.seller_state,
        s.seller_tier,
        date_trunc(f.order_purchase_date, month) as month_date,
        extract(year from f.order_purchase_date) as year,

        -- Calculate volume (cm³) and weight category
        extract(month from f.order_purchase_date) as month,
        format_date('%Y-%m', f.order_purchase_date) as month_name,

        -- Seller info
        (p.product_length_cm * p.product_height_cm * p.product_width_cm) as product_volume_cm3,
        case
            when p.product_weight_g < 500 then 'light'
            when p.product_weight_g < 2000 then 'medium'
            when p.product_weight_g < 10000 then 'heavy'
            else 'very_heavy'
        end as weight_category,

        -- Sales metrics
        count(distinct f.order_id) as total_orders,
        count(distinct f.customer_key) as unique_customers,
        count(distinct oi.seller_id) as unique_sellers,

        -- Revenue metrics
        sum(oi.total_item_value) as total_revenue,
        avg(oi.total_item_value) as avg_item_value,
        sum(oi.price) as total_price,
        sum(oi.freight_value) as total_freight,

        -- Quantity metrics
        sum(oi.order_item_id) as total_items_sold,
        avg(oi.order_item_id) as avg_items_per_order,

        -- Review metrics
        avg(f.review_score) as avg_review_score,
        sum(case when f.review_sentiment = 'positive' then 1 else 0 end) as positive_reviews,
        sum(case when f.review_sentiment = 'neutral' then 1 else 0 end) as neutral_reviews,
        sum(case when f.review_sentiment = 'negative' then 1 else 0 end) as negative_reviews,

        -- Delivery metrics
        avg(f.delivery_days) as avg_delivery_days,
        sum(case when f.is_on_time_delivery then 1 else 0 end) as on_time_deliveries,
        sum(case when f.is_delivered then 1 else 0 end) as delivered_orders

    from {{ ref('fact_orders') }} as f
    inner join {{ ref('stg_order_items') }} as oi
        on f.order_id = oi.order_id
    inner join {{ ref('dim_product') }} as p
        on oi.product_id = p.product_id
    left join {{ ref('dim_seller') }} as s
        on oi.seller_id = s.seller_id

    where f.order_status != 'canceled'

    group by
        date_trunc(f.order_purchase_date, month),
        extract(year from f.order_purchase_date),
        extract(month from f.order_purchase_date),
        format_date('%Y-%m', f.order_purchase_date),
        p.product_id,
        p.product_category_name_en,
        p.product_category_name_pt,
        p.sales_tier,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        (p.product_length_cm * p.product_height_cm * p.product_width_cm),
        case
            when p.product_weight_g < 500 then 'light'
            when p.product_weight_g < 2000 then 'medium'
            when p.product_weight_g < 10000 then 'heavy'
            else 'very_heavy'
        end,
        s.seller_state,
        s.seller_tier
),

with_calculations as (
    select
        *,

        -- Revenue per customer
        round(total_revenue / nullif(unique_customers, 0), 2) as revenue_per_customer,

        -- Average freight percentage
        round(total_freight * 100.0 / nullif(total_revenue, 0), 2) as freight_pct,

        -- Review distribution
        round(positive_reviews * 100.0 / nullif(total_orders, 0), 2) as positive_review_pct,
        round(negative_reviews * 100.0 / nullif(total_orders, 0), 2) as negative_review_pct,

        -- Delivery performance
        round(on_time_deliveries * 100.0 / nullif(delivered_orders, 0), 2) as on_time_delivery_pct,

        -- Items per order ratio
        round(total_items_sold * 1.0 / nullif(total_orders, 0), 2) as items_per_order

    from product_monthly_sales
),

with_rankings as (
    select
        *,

        -- Product rankings within category and month
        row_number() over (
            partition by month_date, product_category
            order by total_revenue desc
        ) as category_revenue_rank,

        row_number() over (
            partition by month_date, product_category
            order by total_orders desc
        ) as category_orders_rank,

        row_number() over (
            partition by month_date, product_category
            order by avg_review_score desc, total_orders desc
        ) as category_rating_rank,

        -- Overall rankings
        row_number() over (
            partition by month_date
            order by total_revenue desc
        ) as overall_revenue_rank,

        -- Month-over-month growth
        round(
            (total_revenue - lag(total_revenue) over (
                partition by product_id
                order by month_date
            )) * 100.0 / nullif(lag(total_revenue) over (
                partition by product_id
                order by month_date
            ), 0),
            2
        ) as mom_revenue_growth_pct,

        round(
            (total_orders - lag(total_orders) over (
                partition by product_id
                order by month_date
            )) * 100.0 / nullif(lag(total_orders) over (
                partition by product_id
                order by month_date
            ), 0),
            2
        ) as mom_orders_growth_pct,

        -- Timestamp
        current_timestamp() as _updated_at

    from with_calculations
)

select * from with_rankings
order by month_date desc, overall_revenue_rank asc
