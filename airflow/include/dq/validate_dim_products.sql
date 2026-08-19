-- Row Count Check
SELECT
CASE
WHEN COUNT(*) > 0 THEN 'PASS'
ELSE ERROR('dim_products is empty')
END
FROM `playground-s-11-82a9d55d.retail_warehouse.dim_products`;

-- Null Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('NULL product_id found')
END
FROM `playground-s-11-82a9d55d.retail_warehouse.dim_products`
WHERE product_id IS NULL;

-- Duplicate Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Duplicate product_id found')
END
FROM (
    SELECT product_id
    FROM `playground-s-11-82a9d55d.retail_warehouse.dim_products`
    GROUP BY product_id
    HAVING COUNT(*) > 1
);