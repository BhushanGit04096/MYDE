-- Row Count Check
SELECT
CASE
WHEN COUNT(*) > 0 THEN 'PASS'
ELSE ERROR('dim_customers is empty')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.dim_customers`;

-- Null Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('NULL customer_id found')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.dim_customers`
WHERE customer_id IS NULL;

-- Duplicate Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Duplicate customer_id found')
END
FROM (
    SELECT customer_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.dim_customers`
    GROUP BY customer_id
    HAVING COUNT(*) > 1
);