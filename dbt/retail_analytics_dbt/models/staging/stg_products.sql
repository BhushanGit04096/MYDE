SELECT
    TRIM(product_id) AS product_id,
    TRIM(product_name) AS product_name,
    INITCAP(TRIM(category)) AS category,
    CAST(unit_price AS NUMERIC) AS unit_price

FROM {{ source('retail_raw', 'raw_products') }}

WHERE product_id IS NOT NULL