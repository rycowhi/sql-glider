-- Merge customer dimension with SCD Type 2 logic
-- Target: dim_customer
-- Sources: stg_customers
-- Pattern: Complex MERGE with UPDATE SET (SCD Type 2)

-- Step 1: Expire changed records (set is_current = false)
MERGE INTO dim_customer AS target
USING (
    SELECT
        dc.customer_sk,
        dc.customer_id,
        sc.customer_name,
        sc.email,
        sc.phone,
        sc.customer_status,
        sc.customer_segment,
        sc.primary_street_address,
        sc.primary_city,
        sc.primary_state,
        sc.primary_postal_code,
        sc.primary_country,
        -- Create hash of all attributes for change detection
        MD5(CONCAT_WS('|',
            sc.customer_name,
            sc.email,
            sc.phone,
            sc.customer_status,
            sc.customer_segment,
            sc.primary_street_address,
            sc.primary_city,
            sc.primary_state,
            sc.primary_postal_code,
            sc.primary_country
        )) AS new_hash
    FROM dim_customer dc
    INNER JOIN stg_customers sc
        ON dc.customer_id = sc.customer_id
    WHERE dc.is_current = TRUE
) AS source
ON target.customer_sk = source.customer_sk
WHEN MATCHED AND target.customer_hash <> source.new_hash THEN
    UPDATE SET
        is_current = FALSE,
        end_date = CURRENT_DATE(),
        updated_timestamp = CURRENT_TIMESTAMP();

-- Step 2: Insert new and changed records
INSERT INTO dim_customer
SELECT
    -- Generate surrogate key (using hash of customer_id and timestamp for uniqueness)
    ABS(HASH(CONCAT(sc.customer_id, CURRENT_TIMESTAMP()))) AS customer_sk,
    sc.customer_id,

    -- Customer attributes
    sc.customer_name,
    sc.email,
    sc.phone,
    sc.registration_date,
    sc.customer_status,
    sc.customer_segment,

    -- Address attributes
    sc.primary_street_address AS street_address,
    sc.primary_city AS city,
    sc.primary_state AS state,
    sc.primary_postal_code AS postal_code,
    sc.primary_country AS country,

    -- Initialize aggregate metrics (will be updated separately)
    0 AS total_orders,
    0.00 AS total_spent,
    0.00 AS total_payments,
    0.00 AS customer_lifetime_value,
    NULL AS first_order_date,
    NULL AS last_order_date,
    NULL AS days_since_last_order,

    -- SCD Type 2 attributes
    TRUE AS is_current,
    CURRENT_DATE() AS start_date,
    NULL AS end_date,
    MD5(CONCAT_WS('|',
        sc.customer_name,
        sc.email,
        sc.phone,
        sc.customer_status,
        sc.customer_segment,
        sc.primary_street_address,
        sc.primary_city,
        sc.primary_state,
        sc.primary_postal_code,
        sc.primary_country
    )) AS customer_hash,

    -- Metadata
    sc.source_system,
    CURRENT_TIMESTAMP() AS created_timestamp,
    CURRENT_TIMESTAMP() AS updated_timestamp

FROM stg_customers sc
LEFT JOIN dim_customer dc
    ON sc.customer_id = dc.customer_id
    AND dc.is_current = TRUE
WHERE
    -- Insert new customers (not in dimension)
    dc.customer_id IS NULL
    OR
    -- Insert changed customers (hash mismatch)
    dc.customer_hash <> MD5(CONCAT_WS('|',
        sc.customer_name,
        sc.email,
        sc.phone,
        sc.customer_status,
        sc.customer_segment,
        sc.primary_street_address,
        sc.primary_city,
        sc.primary_state,
        sc.primary_postal_code,
        sc.primary_country
    ));
