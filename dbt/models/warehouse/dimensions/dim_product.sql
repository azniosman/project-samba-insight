{{
    config(
        materialized='table',
        tags=['dimension', 'product']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

-- Aggregate product metrics from order items
product_metrics as (
    select
        oi.product_id,
        count(distinct oi.order_id) as total_orders,
        sum(1) as total_items_sold,
        sum(oi.price) as total_revenue,
        avg(oi.price) as avg_price,
        min(oi.price) as min_price,
        max(oi.price) as max_price,
        sum(oi.freight_value) as total_freight

    from {{ ref('stg_order_items') }} as oi
    group by oi.product_id
),

-- Get review metrics for products
review_metrics as (
    select
        oi.product_id,
        avg(r.review_score) as avg_review_score,
        count(distinct r.review_id) as total_reviews

    from {{ ref('stg_reviews') }} as r
    inner join {{ ref('stg_order_items') }} as oi
        on r.order_id = oi.order_id
    group by oi.product_id
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,

        -- Natural Key
        p.product_id,

        -- Product Attributes
        p.product_category_name_pt,
        p.product_category_name_en,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,

        -- Physical Dimensions
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.product_volume_cm3,

        -- Sales Metrics
        coalesce(pm.total_orders, 0) as total_orders,
        coalesce(pm.total_items_sold, 0) as total_items_sold,
        coalesce(pm.total_revenue, 0) as total_revenue,
        pm.avg_price,
        pm.min_price,
        pm.max_price,
        coalesce(pm.total_freight, 0) as total_freight,

        -- Review Metrics
        rm.avg_review_score,
        coalesce(rm.total_reviews, 0) as total_reviews,

        -- Product Categorization
        case
            when pm.total_items_sold >= 100 then 'best_seller'
            when pm.total_items_sold >= 50 then 'popular'
            when pm.total_items_sold >= 10 then 'moderate'
            when pm.total_items_sold > 0 then 'low_volume'
            else 'no_sales'
        end as sales_tier,

        -- Data Quality
        p.has_missing_category,
        p.has_missing_dimensions,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from products as p
    left join product_metrics as pm
        on p.product_id = pm.product_id
    left join review_metrics as rm
        on p.product_id = rm.product_id
)

select * from final
