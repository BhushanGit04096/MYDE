-- Row Count Check
SELECT
CASE
WHEN COUNT(*) > 0 THEN 'PASS'
ELSE ERROR('fact_returns is empty')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.fact_returns`;

-- Null Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('NULL return_id found')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.fact_returns`
WHERE return_id IS NULL;

-- Duplicate Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Duplicate return_id found')
END
FROM (
    SELECT return_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.fact_returns`
    GROUP BY return_id
    HAVING COUNT(*) > 1
);

-- Order Referential Integrity
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Invalid order_id found in returns')
END
FROM (
    SELECT r.order_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.fact_returns` r
    LEFT JOIN `playground-s-11-fc547a0e.retail_warehouse.fact_orders` o
    ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
);