{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    c.customer_city,
    c.customer_state,
    CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        THEN 'on_time'
        ELSE 'late'
    END AS delivery_status
FROM {{ ref('bronze_orders') }} o
LEFT JOIN {{ ref('bronze_customers') }} c
    ON o.customer_id = c.customer_id
WHERE o.order_status IS NOT NULL
