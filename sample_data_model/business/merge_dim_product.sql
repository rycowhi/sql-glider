-- Merge product dimension (simple upsert - SCD Type 1)
-- Target: dim_product
-- Sources: stg_products
-- Pattern: Simple MERGE (UPDATE/INSERT)

MERGE INTO dim_product AS target
USING (
    SELECT
        sp.product_id,
        sp.product_name,
        sp.product_description,
        sp.category,
        sp.subcategory,
        sp.brand,
        sp.unit_price,
        sp.cost_price,
        sp.margin_percent,
        sp.currency,
        sp.unit_of_measure,
        sp.product_status,
        sp.is_active,
        sp.launch_date,
        sp.discontinue_date,
        sp.supplier_id
    FROM stg_products sp
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET
        product_name = source.product_name,
        product_description = source.product_description,
        category = source.category,
        subcategory = source.subcategory,
        brand = source.brand,
        unit_price = source.unit_price,
        cost_price = source.cost_price,
        margin_percent = source.margin_percent,
        currency = source.currency,
        unit_of_measure = source.unit_of_measure,
        product_status = source.product_status,
        is_active = source.is_active,
        launch_date = source.launch_date,
        discontinue_date = source.discontinue_date,
        supplier_id = source.supplier_id,
        updated_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        product_sk,
        product_id,
        product_name,
        product_description,
        category,
        subcategory,
        brand,
        unit_price,
        cost_price,
        margin_percent,
        currency,
        unit_of_measure,
        product_status,
        is_active,
        launch_date,
        discontinue_date,
        total_units_sold,
        total_revenue,
        total_returns,
        return_rate,
        supplier_id,
        created_timestamp,
        updated_timestamp
    )
    VALUES (
        ABS(HASH(source.product_id)),  -- Generate surrogate key
        source.product_id,
        source.product_name,
        source.product_description,
        source.category,
        source.subcategory,
        source.brand,
        source.unit_price,
        source.cost_price,
        source.margin_percent,
        source.currency,
        source.unit_of_measure,
        source.product_status,
        source.is_active,
        source.launch_date,
        source.discontinue_date,
        0,  -- Initialize total_units_sold
        0.00,  -- Initialize total_revenue
        0,  -- Initialize total_returns
        0.00,  -- Initialize return_rate
        source.supplier_id,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
