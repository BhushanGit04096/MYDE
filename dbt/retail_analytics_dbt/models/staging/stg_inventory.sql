SELECT
    TRIM(product_id) AS product_id,
    CAST(stock_quantity AS INT64) AS stock_quantity,
    CAST(last_updated AS DATE) AS last_updated

FROM {{ source('retail_raw', 'raw_inventory') }}

WHERE product_id IS NOT NULL