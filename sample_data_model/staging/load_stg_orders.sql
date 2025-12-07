-- Load and denormalize order data (join orders with order items)
-- Target: stg_orders
-- Sources: raw_orders, raw_order_items
-- Pattern: INSERT OVERWRITE (full refresh)

INSERT OVERWRITE TABLE stg_orders
WITH first_orders AS (
    -- Identify each customer's first order
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM raw_orders
    WHERE order_status NOT IN ('cancelled')
    GROUP BY customer_id
)
SELECT
    -- Keys
    ro.order_id,
    roi.order_item_id,
    ro.customer_id,
    roi.product_id,

    -- Order header attributes
    ro.order_date,
    CAST(DATE_FORMAT(ro.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
    LOWER(TRIM(ro.order_status)) AS order_status,
    LOWER(TRIM(ro.order_channel)) AS order_channel,

    -- Order item attributes
    roi.line_number,
    roi.quantity,
    roi.unit_price,
    COALESCE(roi.discount_percent, 0.00) AS discount_percent,
    COALESCE(roi.discount_amount, 0.00) AS discount_amount,
    COALESCE(roi.tax_amount, 0.00) AS tax_amount,
    roi.line_total,

    -- Return information
    COALESCE(roi.return_flag, FALSE) AS return_flag,
    roi.return_date,
    roi.return_reason,

    -- Order totals (from header)
    ro.subtotal_amount AS order_subtotal,
    ro.tax_amount AS order_tax,
    ro.shipping_amount AS order_shipping,
    ro.total_amount AS order_total,

    -- Calculated fields
    CASE
        WHEN roi.return_flag = TRUE THEN 0.00
        ELSE roi.line_total
    END AS net_line_total,
    CASE
        WHEN ro.order_date = fo.first_order_date THEN TRUE
        ELSE FALSE
    END AS is_first_order,

    -- Metadata
    ro.currency,
    CURRENT_TIMESTAMP() AS load_timestamp

FROM raw_orders ro

INNER JOIN raw_order_items roi
    ON ro.order_id = roi.order_id

LEFT JOIN first_orders fo
    ON ro.customer_id = fo.customer_id

WHERE
    ro.order_id IS NOT NULL
    AND roi.order_item_id IS NOT NULL
    AND ro.customer_id IS NOT NULL
    AND roi.product_id IS NOT NULL
    AND roi.quantity > 0
    AND roi.line_total IS NOT NULL;
