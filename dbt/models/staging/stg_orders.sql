{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

with source as (
    select * from {{ source('raw', 'orders_raw') }}
),

cleaned as (
    select
        -- Primary Key
        order_id,

        -- Foreign Keys
        customer_id,

        -- Status
        order_status,

        -- Timestamps
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,

        -- Calculated fields
        timestamp_diff(
            cast(order_delivered_customer_date as timestamp),
            cast(order_purchase_timestamp as timestamp),
            day
        ) as delivery_days,

        timestamp_diff(
            cast(order_delivered_customer_date as timestamp),
            cast(order_estimated_delivery_date as timestamp),
            day
        ) as delivery_delay_days,

        -- Data quality flags
        coalesce(order_delivered_customer_date is not null, false) as is_delivered,

        coalesce(
            order_status = 'delivered'
            and order_delivered_customer_date is null, false
        ) as has_data_quality_issue

    from source
)

select * from cleaned
