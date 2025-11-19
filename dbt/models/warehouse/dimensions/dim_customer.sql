{{
    config(
        materialized='table',
        tags=['dimension', 'customer']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate customer metrics from orders
customer_metrics as (
    select
        c.customer_id,
        count(distinct o.order_id) as total_orders,
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as last_order_date,
        avg(o.delivery_days) as avg_delivery_days,
        sum(case when o.order_status = 'delivered' then 1 else 0 end) as delivered_orders,
        sum(case when o.order_status = 'canceled' then 1 else 0 end) as canceled_orders

    from {{ ref('stg_orders') }} as o
    inner join customers as c
        on o.customer_id = c.customer_id
    group by c.customer_id
),

-- Get review metrics
review_metrics as (
    select
        o.customer_id,
        avg(r.review_score) as avg_review_score,
        count(distinct r.review_id) as total_reviews

    from {{ ref('stg_reviews') }} as r
    inner join {{ ref('stg_orders') }} as o
        on r.order_id = o.order_id
    group by o.customer_id
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,

        -- Natural Key
        c.customer_id,
        c.customer_unique_id,

        -- Location
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,

        -- Order Metrics
        coalesce(cm.total_orders, 0) as total_orders,
        cm.first_order_date,
        cm.last_order_date,
        cm.avg_delivery_days,
        coalesce(cm.delivered_orders, 0) as delivered_orders,
        coalesce(cm.canceled_orders, 0) as canceled_orders,

        -- Review Metrics
        rm.avg_review_score,
        coalesce(rm.total_reviews, 0) as total_reviews,

        -- Customer Segmentation
        case
            when cm.total_orders >= 5 then 'loyal'
            when cm.total_orders >= 2 then 'repeat'
            else 'one_time'
        end as customer_segment,

        -- Data Quality
        c.has_missing_location,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from customers as c
    left join customer_metrics as cm
        on c.customer_id = cm.customer_id
    left join review_metrics as rm
        on c.customer_id = rm.customer_id
)

select * from final
