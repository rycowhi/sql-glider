-- Example multi-query SQL file for testing

SELECT customer_id, customer_name, email
FROM customers;

SELECT order_id, customer_id, order_date, order_total
FROM orders;

INSERT INTO customer_summary
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_total) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;
