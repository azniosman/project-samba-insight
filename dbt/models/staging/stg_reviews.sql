{{
    config(
        materialized='view',
        tags=['staging', 'reviews']
    )
}}

with source as (
    select * from {{ source('raw', 'order_reviews_raw') }}
),

cleaned as (
    select
        -- Primary Key
        review_id,

        -- Foreign Key
        order_id,

        -- Review details
        cast(review_score as int64) as review_score,
        review_comment_title,
        review_comment_message,

        -- Timestamps
        cast(review_creation_date as timestamp) as review_creation_date,
        cast(review_answer_timestamp as timestamp) as review_answer_timestamp,

        -- Calculated fields
        case
            when review_score >= 4 then 'positive'
            when review_score = 3 then 'neutral'
            else 'negative'
        end as review_sentiment,

        length(coalesce(review_comment_message, '')) as comment_length,

        coalesce(
            review_comment_message is not null
            and length(review_comment_message) > 0, false
        ) as has_comment,

        -- Data quality flags
        coalesce(review_score < 1 or review_score > 5, false) as has_invalid_score

    from source
)

select * from cleaned
