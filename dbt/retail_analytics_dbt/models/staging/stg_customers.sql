SELECT
    TRIM(customer_id) AS customer_id,
    TRIM(customer_name) AS customer_name,
    INITCAP(TRIM(city)) AS city,
    INITCAP(TRIM(state)) AS state,
    CAST(signup_date AS DATE) AS signup_date

FROM {{ source('retail_raw', 'raw_customers') }}

WHERE customer_id IS NOT NULL