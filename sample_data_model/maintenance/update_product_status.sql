-- Batch update product status based on business rules
-- Target: dim_product
-- Pattern: UPDATE statement (maintenance operation)
-- Purpose: Mark products as discontinued if no sales in last 365 days

UPDATE dim_product
SET
    product_status = 'discontinued',
    is_active = FALSE,
    discontinue_date = CURRENT_DATE(),
    updated_timestamp = CURRENT_TIMESTAMP()
WHERE
    product_sk IN (
        -- Find products with no recent sales
        SELECT
            dp.product_sk
        FROM dim_product dp
        LEFT JOIN (
            SELECT
                product_sk,
                MAX(DATE(order_timestamp)) AS last_sale_date
            FROM fact_orders
            WHERE order_status IN ('confirmed', 'shipped', 'delivered')
            GROUP BY product_sk
        ) AS recent_sales
            ON dp.product_sk = recent_sales.product_sk
        WHERE
            dp.product_status = 'active'
            AND dp.is_active = TRUE
            AND (
                recent_sales.last_sale_date IS NULL
                OR recent_sales.last_sale_date < DATE_SUB(CURRENT_DATE(), 365)
            )
    );
