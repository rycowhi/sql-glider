-- Customer orders summary report
SELECT
    c.customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_total) AS total_spent,
    MAX(o.order_date) AS last_order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.email;
