-- Load payment fact table with dimension lookups
-- Target: fact_payments
-- Sources: stg_payments, dim_customer
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE fact_payments
SELECT
    -- Generate surrogate key for fact
    ABS(HASH(fp.payment_id)) AS payment_fact_id,
    fp.payment_id,
    fp.order_id,

    -- Dimension keys (lookups)
    dc.customer_sk,
    fp.payment_date_key,

    -- Degenerate dimensions
    fp.payment_method,
    fp.payment_method_category,
    fp.payment_status,
    fp.payment_processor,
    fp.transaction_id,

    -- Payment measures
    fp.payment_amount,
    fp.refund_amount,
    fp.net_payment_amount,

    -- Flags
    fp.has_refund,
    fp.is_valid_payment,
    fp.is_matched_to_order,

    -- Dates and calculated fields
    fp.refund_date,
    CASE
        WHEN fp.refund_date IS NOT NULL
        THEN DATEDIFF(fp.refund_date, DATE(fp.payment_date))
        ELSE NULL
    END AS days_to_refund,

    -- Metadata
    fp.currency,
    fp.payment_date AS payment_timestamp,
    CURRENT_TIMESTAMP() AS created_timestamp

FROM stg_payments fp

-- Join to get customer surrogate key (current record only)
INNER JOIN dim_customer dc
    ON fp.customer_id = dc.customer_id
    AND dc.is_current = TRUE

WHERE
    fp.payment_status NOT IN ('failed');  -- Exclude failed payments
