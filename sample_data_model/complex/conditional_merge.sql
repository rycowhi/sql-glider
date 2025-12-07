-- Complex MERGE with multiple conditional clauses
-- Demonstrates: Advanced MERGE patterns with complex logic
-- Target: dim_product
-- Sources: stg_products, fact_orders

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
        sp.supplier_id,

        -- Calculate recent sales metrics
        COALESCE(recent_sales.units_sold_30d, 0) AS units_sold_30d,
        COALESCE(recent_sales.revenue_30d, 0.00) AS revenue_30d,
        COALESCE(recent_sales.last_sale_date, sp.launch_date) AS last_sale_date

    FROM stg_products sp

    LEFT JOIN (
        SELECT
            fo.product_sk,
            SUM(fo.net_quantity) AS units_sold_30d,
            SUM(fo.net_amount) AS revenue_30d,
            MAX(DATE(fo.order_timestamp)) AS last_sale_date
        FROM fact_orders fo
        WHERE
            fo.order_status IN ('confirmed', 'shipped', 'delivered')
            AND fo.order_date_key >= CAST(DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 30), 'yyyyMMdd') AS INT)
        GROUP BY fo.product_sk
    ) AS recent_sales
        ON target.product_sk = recent_sales.product_sk

) AS source
ON target.product_id = source.product_id

-- Scenario 1: Active product with recent sales - update normally
WHEN MATCHED AND source.is_active = TRUE AND source.units_sold_30d > 0 THEN
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

-- Scenario 2: Active product with NO recent sales - mark as out_of_stock
WHEN MATCHED AND source.is_active = TRUE AND source.units_sold_30d = 0 THEN
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
        product_status = 'out_of_stock',  -- Override status
        is_active = FALSE,  -- Mark as inactive
        launch_date = source.launch_date,
        discontinue_date = source.discontinue_date,
        supplier_id = source.supplier_id,
        updated_timestamp = CURRENT_TIMESTAMP()

-- Scenario 3: Discontinued product - update but don't change status
WHEN MATCHED AND source.is_active = FALSE THEN
    UPDATE SET
        product_name = source.product_name,
        product_description = source.product_description,
        unit_price = source.unit_price,
        cost_price = source.cost_price,
        margin_percent = source.margin_percent,
        discontinue_date = COALESCE(source.discontinue_date, CURRENT_DATE()),
        updated_timestamp = CURRENT_TIMESTAMP()

-- Scenario 4: New product - insert
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
        ABS(HASH(source.product_id)),
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
        0,
        0.00,
        0,
        0.00,
        source.supplier_id,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
