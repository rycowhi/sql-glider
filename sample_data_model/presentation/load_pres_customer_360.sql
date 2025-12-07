-- Build customer 360 view (complete customer profile)
-- Target: pres_customer_360
-- Sources: dim_customer, fact_orders, fact_payments
-- Pattern: INSERT OVERWRITE with complex CTEs

WITH customer_order_summary AS (
    -- Aggregate order metrics per customer
    SELECT
        fo.customer_sk,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(fo.net_amount) AS total_order_value,
        AVG(fo.net_amount) AS avg_order_value,
        MIN(DATE(fo.order_timestamp)) AS first_order_date,
        MAX(DATE(fo.order_timestamp)) AS last_order_date,
        SUM(CASE WHEN fo.return_flag = TRUE THEN 1 ELSE 0 END) AS total_returns,
        SUM(fo.return_amount) AS total_return_value,
        COUNT(DISTINCT fo.order_channel) AS channel_count,
        MODE(fo.order_channel) AS preferred_channel
    FROM fact_orders fo
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY fo.customer_sk
),
customer_payment_summary AS (
    -- Aggregate payment metrics per customer
    SELECT
        fp.customer_sk,
        COUNT(DISTINCT fp.payment_id) AS total_payments,
        SUM(fp.net_payment_amount) AS total_payment_value,
        AVG(fp.net_payment_amount) AS avg_payment_value,
        SUM(CASE WHEN fp.has_refund = TRUE THEN 1 ELSE 0 END) AS total_refunds,
        SUM(fp.refund_amount) AS total_refund_value,
        MODE(fp.payment_method) AS preferred_payment_method,
        MODE(fp.payment_method_category) AS preferred_payment_category
    FROM fact_payments fp
    WHERE fp.is_valid_payment = TRUE
    GROUP BY fp.customer_sk
),
customer_product_preferences AS (
    -- Identify top product categories per customer
    SELECT
        fo.customer_sk,
        dp.category AS top_category,
        SUM(fo.net_amount) AS category_spend,
        ROW_NUMBER() OVER (PARTITION BY fo.customer_sk ORDER BY SUM(fo.net_amount) DESC) AS category_rank
    FROM fact_orders fo
    INNER JOIN dim_product dp
        ON fo.product_sk = dp.product_sk
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY fo.customer_sk, dp.category
)
INSERT OVERWRITE TABLE pres_customer_360
SELECT
    -- Customer identifiers
    dc.customer_sk,
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.phone,

    -- Customer attributes
    dc.customer_status,
    dc.customer_segment,
    dc.registration_date,

    -- Address information
    dc.street_address,
    dc.city,
    dc.state,
    dc.postal_code,
    dc.country,

    -- Order metrics
    COALESCE(cos.total_orders, 0) AS total_orders,
    COALESCE(cos.total_order_value, 0.00) AS total_order_value,
    COALESCE(cos.avg_order_value, 0.00) AS avg_order_value,
    cos.first_order_date,
    cos.last_order_date,
    COALESCE(cos.total_returns, 0) AS total_returns,
    COALESCE(cos.total_return_value, 0.00) AS total_return_value,
    CASE
        WHEN cos.total_orders > 0
        THEN ROUND((cos.total_returns / cos.total_orders) * 100, 2)
        ELSE 0.00
    END AS return_rate_percent,

    -- Payment metrics
    COALESCE(cps.total_payments, 0) AS total_payments,
    COALESCE(cps.total_payment_value, 0.00) AS total_payment_value,
    COALESCE(cps.avg_payment_value, 0.00) AS avg_payment_value,
    COALESCE(cps.total_refunds, 0) AS total_refunds,
    COALESCE(cps.total_refund_value, 0.00) AS total_refund_value,
    cps.preferred_payment_method,
    cps.preferred_payment_category,

    -- Customer behavior
    cos.preferred_channel,
    COALESCE(cos.channel_count, 0) AS channel_count,
    cpp.top_category AS preferred_product_category,

    -- Customer lifetime value
    COALESCE(cos.total_order_value, 0.00) AS customer_lifetime_value,

    -- Customer lifecycle
    CASE
        WHEN cos.last_order_date IS NOT NULL
        THEN DATEDIFF(CURRENT_DATE(), cos.last_order_date)
        ELSE NULL
    END AS days_since_last_order,
    CASE
        WHEN cos.first_order_date IS NOT NULL AND cos.last_order_date IS NOT NULL
        THEN DATEDIFF(cos.last_order_date, cos.first_order_date)
        ELSE 0
    END AS customer_tenure_days,

    -- Customer segmentation
    CASE
        WHEN cos.total_order_value >= 10000 THEN 'VIP'
        WHEN cos.total_order_value >= 5000 THEN 'Premium'
        WHEN cos.total_order_value >= 1000 THEN 'Standard'
        WHEN cos.total_order_value > 0 THEN 'Basic'
        ELSE 'Prospect'
    END AS value_segment,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), cos.last_order_date) <= 30 THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), cos.last_order_date) <= 90 THEN 'At Risk'
        WHEN DATEDIFF(CURRENT_DATE(), cos.last_order_date) <= 180 THEN 'Dormant'
        WHEN DATEDIFF(CURRENT_DATE(), cos.last_order_date) > 180 THEN 'Churned'
        ELSE 'Never Purchased'
    END AS lifecycle_stage,

    -- Metadata
    CURRENT_TIMESTAMP() AS load_timestamp

FROM dim_customer dc

LEFT JOIN customer_order_summary cos
    ON dc.customer_sk = cos.customer_sk

LEFT JOIN customer_payment_summary cps
    ON dc.customer_sk = cps.customer_sk

LEFT JOIN customer_product_preferences cpp
    ON dc.customer_sk = cpp.customer_sk
    AND cpp.category_rank = 1  -- Only top category

WHERE dc.is_current = TRUE;
