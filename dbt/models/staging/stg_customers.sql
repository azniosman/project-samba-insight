{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

with source as (
    select * from {{ source('raw', 'customers_raw') }}
),

cleaned as (
    select
        -- Primary Key
        customer_id,

        -- Unique identifier
        customer_unique_id,

        -- Location fields
        cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,

        -- Data quality
        coalesce(customer_city is null or customer_state is null, false) as has_missing_location

    from source
)

select * from cleaned
