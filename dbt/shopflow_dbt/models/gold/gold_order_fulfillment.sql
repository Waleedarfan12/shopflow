{{ config(materialized='table', schema='gold') }}

SELECT
    order_status,
    delivery_status,
    customer_state,
    COUNT(DISTINCT order_id)    AS total_orders,
    AVG(
        EXTRACT(EPOCH FROM (
            order_delivered_customer_date - order_purchase_timestamp
        )) / 86400
    )                           AS avg_delivery_days
FROM {{ ref('silver_orders') }}
GROUP BY 1, 2, 3
ORDER BY total_orders DESC
