{{ config(materialized='table', schema='silver') }}

SELECT
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS total_amount,
    p.product_category_name,
    s.seller_city,
    s.seller_state
FROM {{ ref('bronze_order_items') }} oi
LEFT JOIN {{ ref('bronze_products') }} p
    ON oi.product_id = p.product_id
LEFT JOIN {{ ref('bronze_sellers') }} s
    ON oi.seller_id = s.seller_id
WHERE oi.price IS NOT NULL
