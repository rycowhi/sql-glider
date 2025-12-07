-- Complex multi-statement transformation pipeline
-- Demonstrates: Multiple DML statements in single file
-- Purpose: Complete end-to-end data pipeline example

-- Statement 1: Refresh staging customers
INSERT OVERWRITE TABLE stg_customers
SELECT
    rc.customer_id,
    TRIM(UPPER(rc.customer_name)) AS customer_name,
    LOWER(TRIM(rc.email)) AS email,
    REGEXP_REPLACE(rc.phone, '[^0-9]', '') AS phone,
    rc.registration_date,
    COALESCE(rc.customer_status, 'unknown') AS customer_status,
    COALESCE(rc.customer_segment, 'retail') AS customer_segment,
    COALESCE(ra.street_address, 'Unknown') AS primary_street_address,
    COALESCE(ra.city, 'Unknown') AS primary_city,
    COALESCE(ra.state, 'Unknown') AS primary_state,
    COALESCE(ra.postal_code, 'Unknown') AS primary_postal_code,
    COALESCE(ra.country, 'Unknown') AS primary_country,
    CASE WHEN rc.email LIKE '%@%.%' THEN TRUE ELSE FALSE END AS is_valid_email,
    CASE WHEN LENGTH(REGEXP_REPLACE(rc.phone, '[^0-9]', '')) >= 10 THEN TRUE ELSE FALSE END AS is_valid_phone,
    CASE WHEN ra.address_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_address,
    rc.source_system,
    CURRENT_TIMESTAMP() AS load_timestamp,
    rc.updated_at
FROM raw_customers rc
LEFT JOIN raw_addresses ra
    ON rc.customer_id = ra.customer_id
    AND ra.is_primary = TRUE
    AND ra.valid_to IS NULL
WHERE rc.customer_id IS NOT NULL
    AND rc.deleted_flag = FALSE
    AND rc.registration_date IS NOT NULL;

-- Statement 2: Update dimension with new/changed customers
MERGE INTO dim_customer AS target
USING (
    SELECT
        sc.customer_id,
        sc.customer_name,
        sc.email,
        sc.phone,
        sc.registration_date,
        sc.customer_status,
        sc.customer_segment,
        sc.primary_street_address,
        sc.primary_city,
        sc.primary_state,
        sc.primary_postal_code,
        sc.primary_country,
        sc.source_system,
        MD5(CONCAT_WS('|',
            sc.customer_name, sc.email, sc.phone, sc.customer_status,
            sc.customer_segment, sc.primary_street_address, sc.primary_city,
            sc.primary_state, sc.primary_postal_code, sc.primary_country
        )) AS customer_hash
    FROM stg_customers sc
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND target.customer_hash <> source.customer_hash THEN
    UPDATE SET
        is_current = FALSE,
        end_date = CURRENT_DATE(),
        updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        customer_sk, customer_id, customer_name, email, phone,
        registration_date, customer_status, customer_segment,
        street_address, city, state, postal_code, country,
        total_orders, total_spent, total_payments, customer_lifetime_value,
        first_order_date, last_order_date, days_since_last_order,
        is_current, start_date, end_date, customer_hash,
        source_system, created_timestamp, updated_timestamp
    )
    VALUES (
        ABS(HASH(CONCAT(source.customer_id, CURRENT_TIMESTAMP()))),
        source.customer_id, source.customer_name, source.email, source.phone,
        source.registration_date, source.customer_status, source.customer_segment,
        source.primary_street_address, source.primary_city, source.primary_state,
        source.primary_postal_code, source.primary_country,
        0, 0.00, 0.00, 0.00, NULL, NULL, NULL,
        TRUE, CURRENT_DATE(), NULL, source.customer_hash,
        source.source_system, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

-- Statement 3: Update customer metrics from facts
UPDATE dim_customer dc
SET
    total_orders = COALESCE(om.order_count, 0),
    total_spent = COALESCE(om.total_revenue, 0.00),
    first_order_date = om.first_order_date,
    last_order_date = om.last_order_date,
    days_since_last_order = CASE
        WHEN om.last_order_date IS NOT NULL
        THEN DATEDIFF(CURRENT_DATE(), om.last_order_date)
        ELSE NULL
    END,
    customer_lifetime_value = COALESCE(om.total_revenue, 0.00),
    updated_timestamp = CURRENT_TIMESTAMP()
FROM (
    SELECT
        customer_sk,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(net_amount) AS total_revenue,
        MIN(DATE(order_timestamp)) AS first_order_date,
        MAX(DATE(order_timestamp)) AS last_order_date
    FROM fact_orders
    WHERE order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY customer_sk
) om
WHERE dc.customer_sk = om.customer_sk
    AND dc.is_current = TRUE;
