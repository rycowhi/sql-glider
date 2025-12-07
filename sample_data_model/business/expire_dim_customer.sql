-- Expire soft-deleted customers in dimension
-- Target: dim_customer
-- Sources: raw_customers
-- Pattern: MERGE with conditional DELETE

MERGE INTO dim_customer AS target
USING (
    SELECT DISTINCT
        dc.customer_sk,
        rc.customer_id
    FROM raw_customers rc
    INNER JOIN dim_customer dc
        ON rc.customer_id = dc.customer_id
        AND dc.is_current = TRUE
    WHERE
        rc.deleted_flag = TRUE  -- Customer marked as deleted in source
) AS source
ON target.customer_sk = source.customer_sk
WHEN MATCHED THEN
    UPDATE SET
        is_current = FALSE,
        end_date = CURRENT_DATE(),
        updated_timestamp = CURRENT_TIMESTAMP();
