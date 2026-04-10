{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
    COUNT(DISTINCT o.order_id)                       AS total_orders,
    SUM(p.payment_value)                             AS total_revenue,
    AVG(p.payment_value)                             AS avg_order_value
FROM {{ ref('silver_orders') }} o
LEFT JOIN {{ ref('silver_payments') }} p
    ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1
