{{ config(materialized='table', schema='silver') }}

SELECT
    order_id,
    payment_type,
    payment_installments,
    payment_value,
    CASE
        WHEN payment_value >= 500 THEN 'high'
        WHEN payment_value >= 100 THEN 'medium'
        ELSE 'low'
    END AS payment_category
FROM {{ ref('bronze_order_payments') }}
WHERE payment_value IS NOT NULL
AND payment_value > 0
