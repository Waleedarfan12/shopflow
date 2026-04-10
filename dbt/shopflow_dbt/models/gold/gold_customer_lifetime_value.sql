{{ config(materialized='table') }}

SELECT
    o.customer_id,
    o.customer_city,
    o.customer_state,
    COUNT(DISTINCT o.order_id)  AS total_orders,
    SUM(p.payment_value)        AS lifetime_value,
    AVG(p.payment_value)        AS avg_order_value,
    MIN(o.order_purchase_timestamp) AS first_order_date,
    MAX(o.order_purchase_timestamp) AS last_order_date
FROM {{ ref('silver_orders') }} o
LEFT JOIN {{ ref('silver_payments') }} p
    ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1, 2, 3
