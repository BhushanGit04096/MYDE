-- Row Count Check
SELECT
CASE
WHEN COUNT(*) > 0 THEN 'PASS'
ELSE ERROR('fact_inventory is empty')
END
FROM `playground-s-11-82a9d55d.retail_warehouse.fact_inventory`;

-- Null Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('NULL product_id found')
END
FROM `playground-s-11-82a9d55d.retail_warehouse.fact_inventory`
WHERE product_id IS NULL;

-- Duplicate Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Duplicate product_id found')
END
FROM (
    SELECT product_id
    FROM `playground-s-11-82a9d55d.retail_warehouse.fact_inventory`
    GROUP BY product_id
    HAVING COUNT(*) > 1
);

-- Product Referential Integrity
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Invalid product_id found in inventory')
END
FROM (
    SELECT i.product_id
    FROM `playground-s-11-82a9d55d.retail_warehouse.fact_inventory` i
    LEFT JOIN `playground-s-11-82a9d55d.retail_warehouse.dim_products` d
    ON i.product_id = d.product_id
    WHERE d.product_id IS NULL
);