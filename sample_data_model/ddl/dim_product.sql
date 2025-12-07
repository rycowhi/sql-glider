-- Product dimension table (SCD Type 1 - overwrites)
-- Populated by: business/merge_dim_product.sql
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk BIGINT COMMENT 'Surrogate key for product dimension',
    product_id BIGINT COMMENT 'Natural key - product identifier',

    -- Product attributes
    product_name STRING COMMENT 'Product name',
    product_description STRING COMMENT 'Product description',
    category STRING COMMENT 'Product category',
    subcategory STRING COMMENT 'Product subcategory',
    brand STRING COMMENT 'Product brand',

    -- Pricing attributes
    unit_price DECIMAL(10, 2) COMMENT 'Current unit price',
    cost_price DECIMAL(10, 2) COMMENT 'Current cost price',
    margin_percent DECIMAL(5, 2) COMMENT 'Margin percentage',
    currency STRING COMMENT 'Currency code',
    unit_of_measure STRING COMMENT 'Unit of measure',

    -- Status attributes
    product_status STRING COMMENT 'Product status',
    is_active BOOLEAN COMMENT 'Active product flag',
    launch_date DATE COMMENT 'Product launch date',
    discontinue_date DATE COMMENT 'Discontinuation date (NULL if active)',

    -- Aggregate metrics (updated separately)
    total_units_sold INT COMMENT 'Total units sold all time',
    total_revenue DECIMAL(12, 2) COMMENT 'Total revenue all time',
    total_returns INT COMMENT 'Total units returned',
    return_rate DECIMAL(5, 2) COMMENT 'Return rate percentage',

    -- Metadata
    supplier_id BIGINT COMMENT 'Supplier identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Product dimension with current product attributes'
PARTITIONED BY (category);
