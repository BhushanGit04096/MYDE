-- Row Count Check
SELECT
CASE
WHEN COUNT(*) > 0 THEN 'PASS'
ELSE ERROR('fact_orders is empty')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.fact_orders`;

-- Null Checks
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('NULL values found in fact_orders')
END
FROM `playground-s-11-fc547a0e.retail_warehouse.fact_orders`
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR product_id IS NULL;

-- Duplicate Order Check
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Duplicate order_id found')
END
FROM (
    SELECT order_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.fact_orders`
    GROUP BY order_id
    HAVING COUNT(*) > 1
);

-- Customer Referential Integrity
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Invalid customer_id found')
END
FROM (
    SELECT f.customer_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.fact_orders` f
    LEFT JOIN `playground-s-11-fc547a0e.retail_warehouse.dim_customers` d
    ON f.customer_id = d.customer_id
    WHERE d.customer_id IS NULL
);

-- Product Referential Integrity
SELECT
CASE
WHEN COUNT(*) = 0 THEN 'PASS'
ELSE ERROR('Invalid product_id found')
END
FROM (
    SELECT f.product_id
    FROM `playground-s-11-fc547a0e.retail_warehouse.fact_orders` f
    LEFT JOIN `playground-s-11-fc547a0e.retail_warehouse.dim_products` d
    ON f.product_id = d.product_id
    WHERE d.product_id IS NULL
);