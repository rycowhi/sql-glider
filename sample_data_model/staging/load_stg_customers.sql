-- Load and clean customer data from raw layer
-- Target: stg_customers
-- Sources: raw_customers, raw_addresses
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE stg_customers
SELECT
    -- Primary key
    rc.customer_id,

    -- Cleaned attributes
    TRIM(UPPER(rc.customer_name)) AS customer_name,
    LOWER(TRIM(rc.email)) AS email,
    REGEXP_REPLACE(rc.phone, '[^0-9]', '') AS phone,
    rc.registration_date,
    COALESCE(rc.customer_status, 'unknown') AS customer_status,
    COALESCE(rc.customer_segment, 'retail') AS customer_segment,

    -- Address information (from primary address)
    COALESCE(ra.street_address, 'Unknown') AS primary_street_address,
    COALESCE(ra.city, 'Unknown') AS primary_city,
    COALESCE(ra.state, 'Unknown') AS primary_state,
    COALESCE(ra.postal_code, 'Unknown') AS primary_postal_code,
    COALESCE(ra.country, 'Unknown') AS primary_country,

    -- Data quality flags
    CASE
        WHEN rc.email LIKE '%@%.%' THEN TRUE
        ELSE FALSE
    END AS is_valid_email,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(rc.phone, '[^0-9]', '')) >= 10 THEN TRUE
        ELSE FALSE
    END AS is_valid_phone,
    CASE
        WHEN ra.address_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_address,

    -- Metadata
    rc.source_system,
    CURRENT_TIMESTAMP() AS load_timestamp,
    rc.updated_at

FROM raw_customers rc

-- Left join to get primary address
LEFT JOIN raw_addresses ra
    ON rc.customer_id = ra.customer_id
    AND ra.is_primary = TRUE
    AND ra.valid_to IS NULL  -- Current address only

WHERE
    rc.customer_id IS NOT NULL
    AND rc.deleted_flag = FALSE  -- Exclude soft-deleted customers
    AND rc.registration_date IS NOT NULL;
