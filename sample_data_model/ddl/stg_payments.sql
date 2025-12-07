-- Staging payment data (cleaned and validated)
-- Populated by: staging/load_stg_payments.sql
CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id BIGINT COMMENT 'Unique payment identifier',
    order_id BIGINT COMMENT 'Order identifier',
    customer_id BIGINT COMMENT 'Customer identifier',
    payment_date TIMESTAMP COMMENT 'Payment transaction timestamp',
    payment_date_key INT COMMENT 'Date key (YYYYMMDD format)',
    payment_method STRING COMMENT 'Standardized payment method',
    payment_method_category STRING COMMENT 'Payment method category: card, digital_wallet, bank_transfer, cash',
    payment_status STRING COMMENT 'Payment status',
    payment_amount DECIMAL(10, 2) COMMENT 'Payment amount',
    currency STRING COMMENT 'Payment currency code',
    transaction_id STRING COMMENT 'External transaction identifier',
    payment_processor STRING COMMENT 'Payment processor name',

    -- Refund information
    refund_amount DECIMAL(10, 2) COMMENT 'Refund amount (NULL if no refund)',
    refund_date TIMESTAMP COMMENT 'Refund timestamp (NULL if no refund)',
    net_payment_amount DECIMAL(10, 2) COMMENT 'Payment amount minus refunds',
    has_refund BOOLEAN COMMENT 'Refund flag',

    -- Data quality flags
    is_valid_payment BOOLEAN COMMENT 'Payment validation flag (status = captured)',
    is_matched_to_order BOOLEAN COMMENT 'Matched to order flag',

    -- Metadata
    load_timestamp TIMESTAMP COMMENT 'ETL load timestamp',
    updated_at TIMESTAMP COMMENT 'Source record last update timestamp'
)
USING DELTA
COMMENT 'Staging table for cleaned and validated payment data'
PARTITIONED BY (payment_date_key);
