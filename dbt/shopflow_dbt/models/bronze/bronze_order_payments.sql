{{ config(materialized='table', schema='bronze') }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('raw', 'raw_order_payments') }}

