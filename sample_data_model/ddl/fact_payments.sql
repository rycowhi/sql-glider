-- Payment fact table with dimension keys
-- Populated by: business/load_fact_payments.sql (full refresh)
-- Updated by: incremental/incr_fact_payments.sql (incremental)
CREATE TABLE IF NOT EXISTS fact_payments (
    payment_fact_id BIGINT COMMENT 'Surrogate key for payment fact',
    payment_id BIGINT COMMENT 'Natural key - payment identifier',
    order_id BIGINT COMMENT 'Natural key - order identifier',

    -- Dimension keys
    customer_sk BIGINT COMMENT 'Foreign key to dim_customer',
    payment_date_key INT COMMENT 'Date key (YYYYMMDD format)',

    -- Degenerate dimensions
    payment_method STRING COMMENT 'Payment method',
    payment_method_category STRING COMMENT 'Payment method category',
    payment_status STRING COMMENT 'Payment status',
    payment_processor STRING COMMENT 'Payment processor',
    transaction_id STRING COMMENT 'External transaction ID',

    -- Payment measures
    payment_amount DECIMAL(10, 2) COMMENT 'Payment amount',
    refund_amount DECIMAL(10, 2) COMMENT 'Refund amount (0 if no refund)',
    net_payment_amount DECIMAL(10, 2) COMMENT 'Payment minus refund',

    -- Flags
    has_refund BOOLEAN COMMENT 'Refund flag',
    is_valid_payment BOOLEAN COMMENT 'Valid payment flag',
    is_matched_to_order BOOLEAN COMMENT 'Matched to order flag',

    -- Dates
    refund_date DATE COMMENT 'Refund date (NULL if no refund)',
    days_to_refund INT COMMENT 'Days between payment and refund (NULL if no refund)',

    -- Metadata
    currency STRING COMMENT 'Currency code',
    payment_timestamp TIMESTAMP COMMENT 'Payment transaction timestamp',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Payment fact table with grain: one row per payment transaction'
PARTITIONED BY (payment_date_key);
