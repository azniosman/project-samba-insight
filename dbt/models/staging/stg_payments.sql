{{
    config(
        materialized='view',
        tags=['staging', 'payments']
    )
}}

with source as (
    select * from {{ source('raw', 'order_payments_raw') }}
),

cleaned as (
    select
        -- Primary Key
        order_id,
        payment_sequential,

        -- Payment details
        cast(payment_installments as int64) as payment_installments,
        cast(payment_value as float64) as payment_value,
        lower(trim(payment_type)) as payment_type,

        -- Data quality flags
        coalesce(payment_value < 0, false) as has_negative_value,

        coalesce(payment_installments < 1, false) as has_invalid_installments

    from source
)

select * from cleaned
