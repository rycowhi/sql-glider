-- Customer dimension table with SCD Type 2 (slowly changing dimension)
-- Populated by: business/merge_dim_customer.sql
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk BIGINT COMMENT 'Surrogate key for customer dimension',
    customer_id BIGINT COMMENT 'Natural key - customer identifier',

    -- Customer attributes
    customer_name STRING COMMENT 'Customer name',
    email STRING COMMENT 'Customer email address',
    phone STRING COMMENT 'Customer phone number',
    registration_date DATE COMMENT 'Customer registration date',
    customer_status STRING COMMENT 'Customer status',
    customer_segment STRING COMMENT 'Customer segment',

    -- Address attributes
    street_address STRING COMMENT 'Primary street address',
    city STRING COMMENT 'Primary city',
    state STRING COMMENT 'Primary state/province',
    postal_code STRING COMMENT 'Primary postal code',
    country STRING COMMENT 'Primary country code',

    -- Aggregate metrics (updated separately)
    total_orders INT COMMENT 'Total number of orders',
    total_spent DECIMAL(12, 2) COMMENT 'Total amount spent',
    total_payments DECIMAL(12, 2) COMMENT 'Total payments made',
    customer_lifetime_value DECIMAL(12, 2) COMMENT 'Calculated CLV',
    first_order_date DATE COMMENT 'First order date',
    last_order_date DATE COMMENT 'Most recent order date',
    days_since_last_order INT COMMENT 'Days since last order',

    -- SCD Type 2 attributes
    is_current BOOLEAN COMMENT 'Current record flag',
    start_date DATE COMMENT 'Record effective start date',
    end_date DATE COMMENT 'Record effective end date (NULL if current)',
    customer_hash STRING COMMENT 'Hash of all attributes for change detection',

    -- Metadata
    source_system STRING COMMENT 'Source system identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Customer dimension with SCD Type 2 history tracking'
PARTITIONED BY (is_current);
