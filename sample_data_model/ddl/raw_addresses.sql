-- Raw customer address data
-- Source: CRM system daily extract
CREATE TABLE IF NOT EXISTS raw_addresses (
    address_id BIGINT COMMENT 'Unique address identifier',
    customer_id BIGINT COMMENT 'Foreign key to customer',
    address_type STRING COMMENT 'Address type: billing, shipping, both',
    street_address STRING COMMENT 'Street address line 1',
    street_address_2 STRING COMMENT 'Street address line 2',
    city STRING COMMENT 'City name',
    state STRING COMMENT 'State/province code',
    postal_code STRING COMMENT 'Postal/ZIP code',
    country STRING COMMENT 'Country code (ISO 3166-1 alpha-2)',
    is_primary BOOLEAN COMMENT 'Primary address flag',
    valid_from DATE COMMENT 'Address valid from date',
    valid_to DATE COMMENT 'Address valid to date (NULL if current)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw customer address information'
PARTITIONED BY (country);
