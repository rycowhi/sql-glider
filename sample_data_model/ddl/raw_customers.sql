-- Raw customer master data from source system
-- Source: CRM system daily extract
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id BIGINT COMMENT 'Unique customer identifier',
    customer_name STRING COMMENT 'Full customer name',
    email STRING COMMENT 'Customer email address',
    phone STRING COMMENT 'Customer phone number',
    registration_date DATE COMMENT 'Date customer registered',
    customer_status STRING COMMENT 'Customer status: active, inactive, suspended',
    customer_segment STRING COMMENT 'Customer segment: retail, wholesale, enterprise',
    deleted_flag BOOLEAN COMMENT 'Soft delete flag',
    source_system STRING COMMENT 'Source system identifier',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw customer master data from CRM system'
PARTITIONED BY (registration_date);
