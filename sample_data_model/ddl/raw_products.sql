-- Raw product catalog data
-- Source: Product management system daily extract
CREATE TABLE IF NOT EXISTS raw_products (
    product_id BIGINT COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Product name',
    product_description STRING COMMENT 'Product description',
    category STRING COMMENT 'Product category',
    subcategory STRING COMMENT 'Product subcategory',
    brand STRING COMMENT 'Product brand name',
    unit_price DECIMAL(10, 2) COMMENT 'Base unit price',
    cost_price DECIMAL(10, 2) COMMENT 'Product cost price',
    currency STRING COMMENT 'Currency code (ISO 4217)',
    unit_of_measure STRING COMMENT 'Unit of measure: each, kg, liter, etc.',
    product_status STRING COMMENT 'Product status: active, discontinued, out_of_stock',
    launch_date DATE COMMENT 'Product launch date',
    discontinue_date DATE COMMENT 'Product discontinuation date (NULL if active)',
    supplier_id BIGINT COMMENT 'Supplier identifier',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw product catalog from product management system'
PARTITIONED BY (category);
