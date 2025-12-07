-- Hard delete expired customer dimension records
-- Target: dim_customer
-- Pattern: DELETE statement (maintenance operation)
-- Purpose: Remove historical records that are older than retention period

DELETE FROM dim_customer
WHERE
    is_current = FALSE
    AND end_date IS NOT NULL
    AND end_date < DATE_SUB(CURRENT_DATE(), 2555)  -- Records older than 7 years
    AND customer_sk NOT IN (
        -- Keep if referenced in fact tables (referential integrity)
        SELECT DISTINCT customer_sk FROM fact_orders
        UNION
        SELECT DISTINCT customer_sk FROM fact_payments
    );
