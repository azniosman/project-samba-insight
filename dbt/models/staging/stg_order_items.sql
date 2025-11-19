{{
    config(
        materialized='view',
        tags=['staging', 'order_items']
    )
}}

with source as (
    select * from {{ source('raw', 'order_items_raw') }}
),

cleaned as (
    select
        -- Composite Primary Key
        order_id,
        order_item_id,

        -- Foreign Keys
        product_id,
        seller_id,

        -- Timestamps
        cast(shipping_limit_date as timestamp) as shipping_limit_date,

        -- Financial fields
        cast(price as float64) as price,
        cast(freight_value as float64) as freight_value,

        -- Calculated fields
        cast(price as float64) + cast(freight_value as float64) as total_item_value,

        -- Data quality flags
        coalesce(price < 0 or freight_value < 0, false) as has_negative_values

    from source
)

select * from cleaned
