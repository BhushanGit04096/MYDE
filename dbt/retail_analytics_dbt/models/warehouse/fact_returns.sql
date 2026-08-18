SELECT
    return_id,
    order_id,
    return_date,
    return_reason
FROM {{ ref('stg_returns') }}