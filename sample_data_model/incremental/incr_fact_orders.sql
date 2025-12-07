-- Incremental load for order fact table
-- Target: fact_orders
-- Sources: stg_orders, dim_customer, dim_product
-- Pattern: MERGE (incremental upsert based on order date)

MERGE INTO fact_orders AS target
USING (
    SELECT
        -- Generate surrogate key for fact
        ABS(HASH(CONCAT(so.order_id, so.order_item_id))) AS order_fact_id,
        so.order_id,
        so.order_item_id,

        -- Dimension keys (lookups)
        dc.customer_sk,
        dp.product_sk,
        so.order_date_key,

        -- Degenerate dimensions
        so.order_status,
        so.order_channel,
        so.line_number,

        -- Order measures
        so.quantity,
        so.unit_price,
        so.discount_percent,
        so.discount_amount,
        so.tax_amount,
        so.line_total,

        -- Return measures
        so.return_flag,
        so.return_date,
        CASE WHEN so.return_flag = TRUE THEN so.quantity ELSE 0 END AS return_quantity,
        CASE WHEN so.return_flag = TRUE THEN so.line_total ELSE 0.00 END AS return_amount,

        -- Calculated measures
        CASE WHEN so.return_flag = TRUE THEN 0 ELSE so.quantity END AS net_quantity,
        so.net_line_total AS net_amount,
        so.is_first_order,

        -- Order header measures
        so.order_subtotal,
        so.order_tax,
        so.order_shipping,
        so.order_total,

        -- Metadata
        so.currency,
        so.order_date AS order_timestamp,
        CURRENT_TIMESTAMP() AS created_timestamp

    FROM stg_orders so

    -- Join to get customer surrogate key (current record only)
    INNER JOIN dim_customer dc
        ON so.customer_id = dc.customer_id
        AND dc.is_current = TRUE

    -- Join to get product surrogate key
    INNER JOIN dim_product dp
        ON so.product_id = dp.product_id

    WHERE
        so.order_status NOT IN ('cancelled')
        -- Only process orders from last 7 days (incremental window)
        AND so.order_date >= DATE_SUB(CURRENT_DATE(), 7)

) AS source
ON target.order_fact_id = source.order_fact_id
WHEN MATCHED THEN
    UPDATE SET
        order_status = source.order_status,
        return_flag = source.return_flag,
        return_date = source.return_date,
        return_quantity = source.return_quantity,
        return_amount = source.return_amount,
        net_quantity = source.net_quantity,
        net_amount = source.net_amount
WHEN NOT MATCHED THEN
    INSERT (
        order_fact_id,
        order_id,
        order_item_id,
        customer_sk,
        product_sk,
        order_date_key,
        order_status,
        order_channel,
        line_number,
        quantity,
        unit_price,
        discount_percent,
        discount_amount,
        tax_amount,
        line_total,
        return_flag,
        return_date,
        return_quantity,
        return_amount,
        net_quantity,
        net_amount,
        is_first_order,
        order_subtotal,
        order_tax,
        order_shipping,
        order_total,
        currency,
        order_timestamp,
        created_timestamp
    )
    VALUES (
        source.order_fact_id,
        source.order_id,
        source.order_item_id,
        source.customer_sk,
        source.product_sk,
        source.order_date_key,
        source.order_status,
        source.order_channel,
        source.line_number,
        source.quantity,
        source.unit_price,
        source.discount_percent,
        source.discount_amount,
        source.tax_amount,
        source.line_total,
        source.return_flag,
        source.return_date,
        source.return_quantity,
        source.return_amount,
        source.net_quantity,
        source.net_amount,
        source.is_first_order,
        source.order_subtotal,
        source.order_tax,
        source.order_shipping,
        source.order_total,
        source.currency,
        source.order_timestamp,
        source.created_timestamp
    );
