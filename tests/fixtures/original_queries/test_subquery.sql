-- Test query with subquery in FROM clause
SELECT
    sub.total_sales,
    sub.product_name,
    c.category_name
FROM (
    SELECT
        product_id,
        product_name,
        SUM(sales_amount) as total_sales
    FROM sales
    GROUP BY product_id, product_name
) sub
JOIN categories c ON sub.product_id = c.product_id
WHERE sub.total_sales > 1000
