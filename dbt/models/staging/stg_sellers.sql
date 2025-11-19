{{
    config(
        materialized='view',
        tags=['staging', 'sellers']
    )
}}

with source as (
    select * from {{ source('raw', 'sellers_raw') }}
),

cleaned as (
    select
        -- Primary Key
        seller_id,

        -- Location fields
        cast(seller_zip_code_prefix as string) as seller_zip_code_prefix,
        lower(trim(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,

        -- Data quality
        coalesce(seller_city is null or seller_state is null, false) as has_missing_location

    from source
)

select * from cleaned
