SELECT
    TRIM(return_id) AS return_id,
    TRIM(order_id) AS order_id,
    CAST(return_date AS DATE) AS return_date,
    TRIM(return_reason) AS return_reason

FROM {{ source('retail_raw', 'raw_returns') }}

WHERE return_id IS NOT NULL