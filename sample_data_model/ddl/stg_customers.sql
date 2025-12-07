-- Staging customer data (cleaned and validated)
-- Populated by: staging/load_stg_customers.sql
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id BIGINT COMMENT 'Unique customer identifier',
    customer_name STRING COMMENT 'Cleaned customer name',
    email STRING COMMENT 'Validated email address (lowercase)',
    phone STRING COMMENT 'Standardized phone number',
    registration_date DATE COMMENT 'Customer registration date',
    customer_status STRING COMMENT 'Customer status: active, inactive, suspended',
    customer_segment STRING COMMENT 'Customer segment: retail, wholesale, enterprise',

    -- Address information (denormalized from raw_addresses)
    primary_street_address STRING COMMENT 'Primary street address',
    primary_city STRING COMMENT 'Primary city',
    primary_state STRING COMMENT 'Primary state/province',
    primary_postal_code STRING COMMENT 'Primary postal code',
    primary_country STRING COMMENT 'Primary country code',

    -- Data quality flags
    is_valid_email BOOLEAN COMMENT 'Email validation flag',
    is_valid_phone BOOLEAN COMMENT 'Phone validation flag',
    has_address BOOLEAN COMMENT 'Has at least one address flag',

    -- Metadata
    source_system STRING COMMENT 'Source system identifier',
    load_timestamp TIMESTAMP COMMENT 'ETL load timestamp',
    updated_at TIMESTAMP COMMENT 'Source record last update timestamp'
)
USING DELTA
COMMENT 'Staging table for cleaned and validated customer data'
PARTITIONED BY (customer_segment);
