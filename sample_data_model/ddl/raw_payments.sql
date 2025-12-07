-- Raw payment transaction data
-- Source: Payment processing system real-time feed
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id BIGINT COMMENT 'Unique payment identifier',
    order_id BIGINT COMMENT 'Foreign key to order',
    customer_id BIGINT COMMENT 'Foreign key to customer',
    payment_date TIMESTAMP COMMENT 'Payment transaction timestamp',
    payment_method STRING COMMENT 'Payment method: credit_card, debit_card, paypal, bank_transfer, cash',
    payment_status STRING COMMENT 'Payment status: pending, authorized, captured, failed, refunded',
    payment_amount DECIMAL(10, 2) COMMENT 'Payment amount',
    currency STRING COMMENT 'Payment currency code (ISO 4217)',
    transaction_id STRING COMMENT 'External transaction identifier',
    payment_processor STRING COMMENT 'Payment processor name',
    card_last_four STRING COMMENT 'Last four digits of card (if applicable)',
    refund_amount DECIMAL(10, 2) COMMENT 'Refund amount (NULL if no refund)',
    refund_date TIMESTAMP COMMENT 'Refund timestamp (NULL if no refund)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw payment transaction data from payment processing system'
PARTITIONED BY (DATE(payment_date));
