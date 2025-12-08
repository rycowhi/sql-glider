-- Test query with CTEs for multi-level lineage
WITH order_totals AS (
    SELECT
        customer_id,
        SUM(order_amount) as total_amount,
        COUNT(*) as order_count
    FROM orders
    GROUP BY customer_id
),
customer_segments AS (
    SELECT
        ot.customer_id,
        c.customer_name,
        c.region,
        ot.total_amount,
        ot.order_count,
        CASE
            WHEN ot.total_amount > 10000 THEN 'Premium'
            WHEN ot.total_amount > 5000 THEN 'Standard'
            ELSE 'Basic'
        END as segment
    FROM order_totals ot
    JOIN customers c ON ot.customer_id = c.customer_id
)
INSERT INTO TARGET_TABLE
SELECT
    customer_name,
    region,
    segment,
    total_amount
FROM customer_segments
WHERE segment = 'Premium'
