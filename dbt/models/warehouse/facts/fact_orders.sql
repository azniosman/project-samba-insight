{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail',
        tags=['fact', 'orders'],
        partition_by={
            "field": "order_purchase_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['customer_key', 'order_status']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where order_purchase_timestamp > (select max(order_purchase_timestamp) from {{ this }})
    {% endif %}
),

order_items_agg as (
    select
        order_id,
        count(distinct product_id) as total_products,
        count(distinct seller_id) as total_sellers,
        sum(price) as total_price,
        sum(freight_value) as total_freight,
        sum(price + freight_value) as total_order_value

    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
    group by order_id
),

payments_agg as (
    select
        order_id,
        count(distinct payment_type) as payment_methods_count,
        max(payment_installments) as max_installments,
        sum(payment_value) as total_payment_value,
        string_agg(distinct payment_type, ', ' order by payment_type) as payment_types

    from {{ ref('stg_payments') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
    group by order_id
),

reviews as (
    select
        order_id,
        avg(review_score) as review_score,
        max(review_sentiment) as review_sentiment,
        max(case when has_comment then 1 else 0 end) = 1 as has_comment

    from {{ ref('stg_reviews') }}
    {% if is_incremental() %}
        where order_id in (select order_id from orders)
    {% endif %}
    group by order_id
),

customers as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customer') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,

        -- Natural Key
        o.order_id,

        -- Foreign Keys (Dimensions)
        c.customer_key,

        -- Date Keys
        {{ dbt_utils.generate_surrogate_key(['cast(o.order_purchase_timestamp as date)']) }} as purchase_date_key,
        {{ dbt_utils.generate_surrogate_key(['cast(o.order_approved_at as date)']) }} as approved_date_key,
        {{ dbt_utils.generate_surrogate_key(['cast(o.order_delivered_customer_date as date)']) }} as delivered_date_key,

        -- Order Attributes
        o.order_status,

        -- Timestamps
        o.order_purchase_timestamp,
        cast(o.order_purchase_timestamp as date) as order_purchase_date,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- Delivery Metrics
        o.delivery_days,
        o.delivery_delay_days,
        o.is_delivered,

        -- Order Item Metrics
        coalesce(oi.total_products, 0) as total_products,
        coalesce(oi.total_sellers, 0) as total_sellers,
        coalesce(oi.total_price, 0) as total_price,
        coalesce(oi.total_freight, 0) as total_freight,
        coalesce(oi.total_order_value, 0) as total_order_value,

        -- Payment Metrics
        coalesce(p.payment_methods_count, 0) as payment_methods_count,
        coalesce(p.max_installments, 1) as max_installments,
        coalesce(p.total_payment_value, 0) as total_payment_value,
        p.payment_types,

        -- Review Metrics
        r.review_score,
        r.review_sentiment,
        r.has_comment as has_review_comment,

        -- Calculated Fields
        case
            when o.order_status = 'delivered'
                and o.delivery_delay_days <= 0 then true
            else false
        end as is_on_time_delivery,

        case
            when o.order_status = 'delivered'
                and o.delivery_delay_days > 0 then true
            else false
        end as is_late_delivery,

        -- Payment vs Order Value Check
        case
            when abs(coalesce(p.total_payment_value, 0) - coalesce(oi.total_order_value, 0)) > 0.01
            then true
            else false
        end as has_payment_mismatch,

        -- Data Quality
        o.has_data_quality_issue,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from orders as o
    left join order_items_agg as oi
        on o.order_id = oi.order_id
    left join payments_agg as p
        on o.order_id = p.order_id
    left join reviews as r
        on o.order_id = r.order_id
    inner join customers as c
        on o.customer_id = c.customer_id
)

select * from final
