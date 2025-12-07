-- Incremental load for payment fact table
-- Target: fact_payments
-- Sources: stg_payments, dim_customer
-- Pattern: MERGE (incremental upsert based on payment date)

MERGE INTO fact_payments AS target
USING (
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
        fp.payment_status NOT IN ('failed')
        -- Only process payments from last 7 days (incremental window)
        AND fp.payment_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), 7))

) AS source
ON target.payment_fact_id = source.payment_fact_id
WHEN MATCHED THEN
    UPDATE SET
        payment_status = source.payment_status,
        refund_amount = source.refund_amount,
        net_payment_amount = source.net_payment_amount,
        has_refund = source.has_refund,
        refund_date = source.refund_date,
        days_to_refund = source.days_to_refund
WHEN NOT MATCHED THEN
    INSERT (
        payment_fact_id,
        payment_id,
        order_id,
        customer_sk,
        payment_date_key,
        payment_method,
        payment_method_category,
        payment_status,
        payment_processor,
        transaction_id,
        payment_amount,
        refund_amount,
        net_payment_amount,
        has_refund,
        is_valid_payment,
        is_matched_to_order,
        refund_date,
        days_to_refund,
        currency,
        payment_timestamp,
        created_timestamp
    )
    VALUES (
        source.payment_fact_id,
        source.payment_id,
        source.order_id,
        source.customer_sk,
        source.payment_date_key,
        source.payment_method,
        source.payment_method_category,
        source.payment_status,
        source.payment_processor,
        source.transaction_id,
        source.payment_amount,
        source.refund_amount,
        source.net_payment_amount,
        source.has_refund,
        source.is_valid_payment,
        source.is_matched_to_order,
        source.refund_date,
        source.days_to_refund,
        source.currency,
        source.payment_timestamp,
        source.created_timestamp
    );
