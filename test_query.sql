-- Sample SparkSQL query for testing lineage
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.order_total,
    o.order_date,
    c.region
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
