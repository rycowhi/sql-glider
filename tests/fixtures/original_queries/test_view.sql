CREATE VIEW analytics.customer_summary AS SELECT c.id, c.name, SUM(o.amount) AS total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name
