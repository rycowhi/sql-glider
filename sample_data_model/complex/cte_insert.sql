-- Complex CTE-based INSERT with multiple levels
-- Demonstrates: Deep CTE nesting, window functions, complex joins
-- Target: pres_customer_360 (alternative build approach)

WITH customer_base AS (
    -- Level 1: Get all current customers
    SELECT
        dc.customer_sk,
        dc.customer_id,
        dc.customer_name,
        dc.email,
        dc.phone,
        dc.customer_status,
        dc.customer_segment,
        dc.registration_date,
        dc.street_address,
        dc.city,
        dc.state,
        dc.postal_code,
        dc.country
    FROM dim_customer dc
    WHERE dc.is_current = TRUE
),
order_aggregates AS (
    -- Level 2: Aggregate all order metrics
    SELECT
        fo.customer_sk,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(fo.net_amount) AS total_revenue,
        AVG(fo.net_amount) AS avg_order_value,
        SUM(fo.net_quantity) AS total_units_purchased,
        SUM(CASE WHEN fo.return_flag = TRUE THEN 1 ELSE 0 END) AS return_count,
        SUM(fo.return_amount) AS return_revenue,
        MIN(DATE(fo.order_timestamp)) AS first_order_date,
        MAX(DATE(fo.order_timestamp)) AS last_order_date
    FROM fact_orders fo
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY fo.customer_sk
),
payment_aggregates AS (
    -- Level 2: Aggregate all payment metrics
    SELECT
        fp.customer_sk,
        COUNT(DISTINCT fp.payment_id) AS total_payments,
        SUM(fp.net_payment_amount) AS total_payment_value,
        AVG(fp.net_payment_amount) AS avg_payment_value,
        SUM(CASE WHEN fp.has_refund = TRUE THEN 1 ELSE 0 END) AS refund_count,
        SUM(fp.refund_amount) AS total_refunds
    FROM fact_payments fp
    WHERE fp.is_valid_payment = TRUE
    GROUP BY fp.customer_sk
),
product_preferences AS (
    -- Level 2: Calculate product preferences per customer
    SELECT
        fo.customer_sk,
        dp.category,
        dp.brand,
        SUM(fo.net_amount) AS category_spend,
        SUM(fo.net_quantity) AS category_units,
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_sk
            ORDER BY SUM(fo.net_amount) DESC
        ) AS category_rank
    FROM fact_orders fo
    INNER JOIN dim_product dp ON fo.product_sk = dp.product_sk
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY fo.customer_sk, dp.category, dp.brand
),
top_preferences AS (
    -- Level 3: Filter to top preference only
    SELECT
        customer_sk,
        category AS preferred_category,
        brand AS preferred_brand,
        category_spend,
        category_units
    FROM product_preferences
    WHERE category_rank = 1
),
channel_preferences AS (
    -- Level 2: Calculate channel usage
    SELECT
        fo.customer_sk,
        fo.order_channel,
        COUNT(DISTINCT fo.order_id) AS channel_orders,
        SUM(fo.net_amount) AS channel_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_sk
            ORDER BY COUNT(DISTINCT fo.order_id) DESC
        ) AS channel_rank
    FROM fact_orders fo
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY fo.customer_sk, fo.order_channel
),
top_channel AS (
    -- Level 3: Filter to preferred channel
    SELECT
        customer_sk,
        order_channel AS preferred_channel,
        channel_orders,
        channel_revenue
    FROM channel_preferences
    WHERE channel_rank = 1
),
customer_rfm AS (
    -- Level 3: Calculate RFM (Recency, Frequency, Monetary) scores
    SELECT
        oa.customer_sk,
        DATEDIFF(CURRENT_DATE(), oa.last_order_date) AS recency_days,
        oa.total_orders AS frequency,
        oa.total_revenue AS monetary,
        NTILE(5) OVER (ORDER BY DATEDIFF(CURRENT_DATE(), oa.last_order_date) DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY oa.total_orders ASC) AS frequency_score,
        NTILE(5) OVER (ORDER BY oa.total_revenue ASC) AS monetary_score
    FROM order_aggregates oa
),
final_enrichment AS (
    -- Level 4: Combine all CTEs and add derived metrics
    SELECT
        cb.customer_sk,
        cb.customer_id,
        cb.customer_name,
        cb.email,
        cb.phone,
        cb.customer_status,
        cb.customer_segment,
        cb.registration_date,
        cb.street_address,
        cb.city,
        cb.state,
        cb.postal_code,
        cb.country,

        -- Order metrics
        COALESCE(oa.total_orders, 0) AS total_orders,
        COALESCE(oa.total_revenue, 0.00) AS total_revenue,
        COALESCE(oa.avg_order_value, 0.00) AS avg_order_value,
        COALESCE(oa.total_units_purchased, 0) AS total_units_purchased,
        oa.first_order_date,
        oa.last_order_date,

        -- Return metrics
        COALESCE(oa.return_count, 0) AS return_count,
        COALESCE(oa.return_revenue, 0.00) AS return_revenue,
        CASE
            WHEN oa.total_orders > 0
            THEN ROUND((oa.return_count / oa.total_orders) * 100, 2)
            ELSE 0.00
        END AS return_rate_percent,

        -- Payment metrics
        COALESCE(pa.total_payments, 0) AS total_payments,
        COALESCE(pa.total_payment_value, 0.00) AS total_payment_value,
        COALESCE(pa.refund_count, 0) AS refund_count,
        COALESCE(pa.total_refunds, 0.00) AS total_refunds,

        -- Preferences
        tp.preferred_category,
        tp.preferred_brand,
        tc.preferred_channel,

        -- RFM scores
        COALESCE(rfm.recency_days, 999999) AS days_since_last_order,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        CONCAT(
            CAST(rfm.recency_score AS STRING),
            CAST(rfm.frequency_score AS STRING),
            CAST(rfm.monetary_score AS STRING)
        ) AS rfm_segment,

        -- Customer lifetime value
        COALESCE(oa.total_revenue, 0.00) AS customer_lifetime_value,

        -- Lifecycle stage
        CASE
            WHEN oa.last_order_date IS NULL THEN 'Never Purchased'
            WHEN DATEDIFF(CURRENT_DATE(), oa.last_order_date) <= 30 THEN 'Active'
            WHEN DATEDIFF(CURRENT_DATE(), oa.last_order_date) <= 90 THEN 'At Risk'
            WHEN DATEDIFF(CURRENT_DATE(), oa.last_order_date) <= 180 THEN 'Dormant'
            ELSE 'Churned'
        END AS lifecycle_stage,

        -- Value tier
        CASE
            WHEN oa.total_revenue >= 10000 THEN 'VIP'
            WHEN oa.total_revenue >= 5000 THEN 'Premium'
            WHEN oa.total_revenue >= 1000 THEN 'Standard'
            WHEN oa.total_revenue > 0 THEN 'Basic'
            ELSE 'Prospect'
        END AS value_tier

    FROM customer_base cb
    LEFT JOIN order_aggregates oa ON cb.customer_sk = oa.customer_sk
    LEFT JOIN payment_aggregates pa ON cb.customer_sk = pa.customer_sk
    LEFT JOIN top_preferences tp ON cb.customer_sk = tp.customer_sk
    LEFT JOIN top_channel tc ON cb.customer_sk = tc.customer_sk
    LEFT JOIN customer_rfm rfm ON cb.customer_sk = rfm.customer_sk
)

-- Final INSERT with all enriched data
INSERT OVERWRITE TABLE pres_customer_360
SELECT
    customer_sk,
    customer_id,
    customer_name,
    email,
    phone,
    customer_status,
    customer_segment,
    registration_date,
    street_address,
    city,
    state,
    postal_code,
    country,
    total_orders,
    total_revenue AS total_order_value,
    avg_order_value,
    first_order_date,
    last_order_date,
    return_count AS total_returns,
    return_revenue AS total_return_value,
    return_rate_percent,
    total_payments,
    total_payment_value,
    avg_order_value AS avg_payment_value,
    refund_count AS total_refunds,
    total_refunds AS total_refund_value,
    NULL AS preferred_payment_method,
    NULL AS preferred_payment_category,
    preferred_channel,
    1 AS channel_count,
    preferred_category AS preferred_product_category,
    customer_lifetime_value,
    days_since_last_order,
    DATEDIFF(COALESCE(last_order_date, CURRENT_DATE()), first_order_date) AS customer_tenure_days,
    value_tier AS value_segment,
    lifecycle_stage,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM final_enrichment;
