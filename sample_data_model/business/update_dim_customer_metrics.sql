-- Update customer dimension aggregate metrics
-- Target: dim_customer
-- Sources: fact_orders, fact_payments
-- Pattern: MERGE with UPDATE SET (update only, no inserts)

MERGE INTO dim_customer AS target
USING (
    SELECT
        dc.customer_sk,
        dc.customer_id,

        -- Order metrics
        COALESCE(order_metrics.total_orders, 0) AS total_orders,
        COALESCE(order_metrics.total_spent, 0.00) AS total_spent,
        order_metrics.first_order_date,
        order_metrics.last_order_date,

        -- Payment metrics
        COALESCE(payment_metrics.total_payments, 0.00) AS total_payments,

        -- Calculated CLV (simple version: total spent)
        COALESCE(order_metrics.total_spent, 0.00) AS customer_lifetime_value,

        -- Days since last order
        CASE
            WHEN order_metrics.last_order_date IS NOT NULL
            THEN DATEDIFF(CURRENT_DATE(), order_metrics.last_order_date)
            ELSE NULL
        END AS days_since_last_order

    FROM dim_customer dc

    -- Aggregate order metrics
    LEFT JOIN (
        SELECT
            fo.customer_sk,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            SUM(fo.net_amount) AS total_spent,
            MIN(DATE(fo.order_timestamp)) AS first_order_date,
            MAX(DATE(fo.order_timestamp)) AS last_order_date
        FROM fact_orders fo
        WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
        GROUP BY fo.customer_sk
    ) AS order_metrics
        ON dc.customer_sk = order_metrics.customer_sk

    -- Aggregate payment metrics
    LEFT JOIN (
        SELECT
            fp.customer_sk,
            SUM(fp.net_payment_amount) AS total_payments
        FROM fact_payments fp
        WHERE fp.is_valid_payment = TRUE
        GROUP BY fp.customer_sk
    ) AS payment_metrics
        ON dc.customer_sk = payment_metrics.customer_sk

    WHERE dc.is_current = TRUE

) AS source
ON target.customer_sk = source.customer_sk
WHEN MATCHED THEN
    UPDATE SET
        total_orders = source.total_orders,
        total_spent = source.total_spent,
        total_payments = source.total_payments,
        customer_lifetime_value = source.customer_lifetime_value,
        first_order_date = source.first_order_date,
        last_order_date = source.last_order_date,
        days_since_last_order = source.days_since_last_order,
        updated_timestamp = CURRENT_TIMESTAMP();
