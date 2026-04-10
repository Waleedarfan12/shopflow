{{ config(materialized='table') }}

SELECT
    product_category_name,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(price)                  AS total_revenue,
    AVG(price)                  AS avg_price,
    SUM(freight_value)          AS total_freight
FROM {{ ref('silver_order_items') }}
WHERE product_category_name IS NOT NULL
GROUP BY 1
ORDER BY total_revenue DESC
