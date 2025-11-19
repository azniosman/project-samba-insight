{{
    config(
        materialized='table',
        tags=['dimension', 'seller']
    )
}}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Aggregate seller metrics from order items
seller_metrics as (
    select
        oi.seller_id,
        count(distinct oi.order_id) as total_orders,
        sum(1) as total_items_sold,
        sum(oi.price) as total_revenue,
        avg(oi.price) as avg_item_price,
        sum(oi.freight_value) as total_freight,
        count(distinct oi.product_id) as unique_products_sold

    from {{ ref('stg_order_items') }} as oi
    group by oi.seller_id
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }} as seller_key,

        -- Natural Key
        s.seller_id,

        -- Location
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,

        -- Sales Metrics
        coalesce(sm.total_orders, 0) as total_orders,
        coalesce(sm.total_items_sold, 0) as total_items_sold,
        coalesce(sm.total_revenue, 0) as total_revenue,
        sm.avg_item_price,
        coalesce(sm.total_freight, 0) as total_freight,
        coalesce(sm.unique_products_sold, 0) as unique_products_sold,

        -- Seller Categorization
        case
            when sm.total_items_sold >= 100 then 'high_volume'
            when sm.total_items_sold >= 50 then 'medium_volume'
            when sm.total_items_sold >= 10 then 'low_volume'
            when sm.total_items_sold > 0 then 'occasional'
            else 'no_sales'
        end as seller_tier,

        -- Data Quality
        s.has_missing_location,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from sellers as s
    left join seller_metrics as sm
        on s.seller_id = sm.seller_id
)

select * from final
