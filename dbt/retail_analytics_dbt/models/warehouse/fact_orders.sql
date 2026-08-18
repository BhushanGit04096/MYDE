SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    order_amount
FROM {{ ref('stg_orders') }}