WITH order_totals AS (SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id) SELECT c.name, ot.total FROM customers c JOIN order_totals ot ON c.id = ot.customer_id
