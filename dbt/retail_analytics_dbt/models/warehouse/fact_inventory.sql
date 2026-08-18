SELECT
    product_id,
    stock_quantity,
    last_updated
FROM {{ ref('stg_inventory') }}