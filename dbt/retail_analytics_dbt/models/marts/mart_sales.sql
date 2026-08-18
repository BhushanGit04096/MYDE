SELECT
    fo.order_id,
    fo.order_date,

    dc.customer_id,
    dc.customer_name,
    dc.city,
    dc.state,

    dp.product_id,
    dp.product_name,
    dp.category,

    fo.quantity,
    dp.unit_price,
    fo.order_amount

FROM {{ ref('fact_orders') }} fo

LEFT JOIN {{ ref('dim_customers') }} dc
    ON fo.customer_id = dc.customer_id

LEFT JOIN {{ ref('dim_products') }} dp
    ON fo.product_id = dp.product_id