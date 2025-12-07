-- Load order fact table with dimension lookups
-- Target: fact_orders
-- Sources: stg_orders, dim_customer, dim_product
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE fact_orders
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
    so.order_status NOT IN ('cancelled');  -- Exclude cancelled orders
