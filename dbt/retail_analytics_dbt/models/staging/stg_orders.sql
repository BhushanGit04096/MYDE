SELECT
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    TRIM(product_id) AS product_id,
    CAST(quantity AS INT64) AS quantity,
    CAST(order_date AS DATE) AS order_date,
    CAST(order_amount AS NUMERIC) AS order_amount

FROM {{ source('retail_raw', 'raw_orders') }}

WHERE order_id IS NOT NULL