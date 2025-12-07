-- Staging product data (cleaned and validated)
-- Populated by: staging/load_stg_products.sql
CREATE TABLE IF NOT EXISTS stg_products (
    product_id BIGINT COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Cleaned product name',
    product_description STRING COMMENT 'Product description',
    category STRING COMMENT 'Product category (standardized)',
    subcategory STRING COMMENT 'Product subcategory (standardized)',
    brand STRING COMMENT 'Product brand name (standardized)',
    unit_price DECIMAL(10, 2) COMMENT 'Current unit price',
    cost_price DECIMAL(10, 2) COMMENT 'Current cost price',
    margin_percent DECIMAL(5, 2) COMMENT 'Calculated margin percentage',
    currency STRING COMMENT 'Currency code (ISO 4217)',
    unit_of_measure STRING COMMENT 'Standardized unit of measure',
    product_status STRING COMMENT 'Product status: active, discontinued, out_of_stock',
    launch_date DATE COMMENT 'Product launch date',
    discontinue_date DATE COMMENT 'Product discontinuation date (NULL if active)',
    days_since_launch INT COMMENT 'Calculated days since product launch',
    is_active BOOLEAN COMMENT 'Active product flag (status = active)',

    -- Metadata
    supplier_id BIGINT COMMENT 'Supplier identifier',
    load_timestamp TIMESTAMP COMMENT 'ETL load timestamp',
    updated_at TIMESTAMP COMMENT 'Source record last update timestamp'
)
USING DELTA
COMMENT 'Staging table for cleaned and validated product data'
PARTITIONED BY (category);
