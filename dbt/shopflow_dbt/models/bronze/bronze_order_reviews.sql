{{ config(materialized='table' ) }}

SELECT
    review_id,
    order_id,
    review_score,
    review_creation_date::timestamp,
    review_answer_timestamp::timestamp
FROM {{ source('raw', 'raw_order_reviews') }}
