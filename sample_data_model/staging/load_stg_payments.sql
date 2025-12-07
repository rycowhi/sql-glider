-- Load and clean payment data from raw layer
-- Target: stg_payments
-- Sources: raw_payments
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE stg_payments
SELECT
    -- Keys
    payment_id,
    order_id,
    customer_id,

    -- Payment attributes
    payment_date,
    CAST(DATE_FORMAT(payment_date, 'yyyyMMdd') AS INT) AS payment_date_key,
    LOWER(TRIM(payment_method)) AS payment_method,

    -- Categorize payment methods
    CASE
        WHEN LOWER(payment_method) IN ('credit_card', 'debit_card') THEN 'card'
        WHEN LOWER(payment_method) IN ('paypal', 'apple_pay', 'google_pay') THEN 'digital_wallet'
        WHEN LOWER(payment_method) = 'bank_transfer' THEN 'bank_transfer'
        WHEN LOWER(payment_method) = 'cash' THEN 'cash'
        ELSE 'other'
    END AS payment_method_category,

    LOWER(TRIM(payment_status)) AS payment_status,
    payment_amount,
    COALESCE(currency, 'USD') AS currency,
    transaction_id,
    payment_processor,

    -- Refund information
    COALESCE(refund_amount, 0.00) AS refund_amount,
    refund_date,
    payment_amount - COALESCE(refund_amount, 0.00) AS net_payment_amount,
    CASE
        WHEN refund_amount IS NOT NULL AND refund_amount > 0 THEN TRUE
        ELSE FALSE
    END AS has_refund,

    -- Data quality flags
    CASE
        WHEN LOWER(payment_status) = 'captured' THEN TRUE
        ELSE FALSE
    END AS is_valid_payment,
    CASE
        WHEN order_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_matched_to_order,

    -- Metadata
    CURRENT_TIMESTAMP() AS load_timestamp,
    updated_at

FROM raw_payments

WHERE
    payment_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND payment_date IS NOT NULL
    AND payment_amount >= 0
    AND payment_status IS NOT NULL;
