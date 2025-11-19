{{
    config(
        materialized='view',
        tags=['staging', 'products']
    )
}}

with source as (
    select * from {{ source('raw', 'products_raw') }}
),

cleaned as (
    select
        -- Primary Key
        p.product_id,

        -- Category
        p.product_category_name as product_category_name_pt,
        cast(p.product_name_lenght as float64) as product_name_length,

        -- Dimensions (in cm)
        cast(p.product_description_lenght as float64) as product_description_length,
        cast(p.product_photos_qty as int64) as product_photos_qty,
        cast(p.product_weight_g as float64) as product_weight_g,
        cast(p.product_length_cm as float64) as product_length_cm,
        cast(p.product_height_cm as float64) as product_height_cm,
        cast(p.product_width_cm as float64) as product_width_cm,
        coalesce(p.product_category_name, 'uncategorized') as product_category_name_en,

        -- Calculated: Volume in cubic cm
        cast(p.product_length_cm as float64)
        * cast(p.product_height_cm as float64)
        * cast(p.product_width_cm as float64) as product_volume_cm3,

        -- Data quality flags
        coalesce(p.product_category_name is null, false) as has_missing_category,

        coalesce(
            p.product_weight_g is null
            or p.product_length_cm is null
            or p.product_height_cm is null
            or p.product_width_cm is null, false
        ) as has_missing_dimensions

    from source as p
)

select * from cleaned
