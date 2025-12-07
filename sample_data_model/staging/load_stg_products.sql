-- Load and clean product data from raw layer
-- Target: stg_products
-- Sources: raw_products
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE stg_products
SELECT
    -- Primary key
    product_id,

    -- Cleaned attributes
    TRIM(product_name) AS product_name,
    TRIM(product_description) AS product_description,
    UPPER(TRIM(category)) AS category,
    UPPER(TRIM(subcategory)) AS subcategory,
    TRIM(brand) AS brand,

    -- Pricing
    COALESCE(unit_price, 0.00) AS unit_price,
    COALESCE(cost_price, 0.00) AS cost_price,
    ROUND(((unit_price - cost_price) / NULLIF(unit_price, 0)) * 100, 2) AS margin_percent,
    COALESCE(currency, 'USD') AS currency,
    LOWER(TRIM(unit_of_measure)) AS unit_of_measure,

    -- Status
    LOWER(TRIM(product_status)) AS product_status,
    launch_date,
    discontinue_date,

    -- Calculated fields
    DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch,
    CASE
        WHEN LOWER(product_status) = 'active' THEN TRUE
        ELSE FALSE
    END AS is_active,

    -- Metadata
    supplier_id,
    CURRENT_TIMESTAMP() AS load_timestamp,
    updated_at

FROM raw_products

WHERE
    product_id IS NOT NULL
    AND product_name IS NOT NULL
    AND unit_price >= 0
    AND cost_price >= 0
    AND launch_date IS NOT NULL;
