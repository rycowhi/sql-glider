-- Complex analytics pipeline with MERGE, UNIONs, window functions, and UDF
-- SparkSQL dialect

-- Register a custom UDF for customer tier classification
CREATE TEMPORARY FUNCTION classify_customer_tier AS 'com.example.udfs.CustomerTierClassifier';

-- CTE-based analytics with window functions and partitioning
WITH customer_order_metrics AS (
    -- Calculate customer-level metrics with window functions
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        o.order_id,
        o.order_total,
        o.order_date,
        o.status,
        -- Window functions with PARTITION BY and ORDER BY
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date DESC
        ) AS order_recency_rank,
        RANK() OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_total DESC
        ) AS order_value_rank,
        DENSE_RANK() OVER (
            ORDER BY o.order_total DESC
        ) AS global_order_rank,
        SUM(o.order_total) OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_spend,
        AVG(o.order_total) OVER (
            PARTITION BY c.customer_id
        ) AS avg_order_value,
        COUNT(o.order_id) OVER (
            PARTITION BY c.customer_id
        ) AS total_orders,
        FIRST_VALUE(o.order_date) OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date
        ) AS first_order_date,
        LAST_VALUE(o.order_date) OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_order_date,
        LAG(o.order_total, 1, 0) OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date
        ) AS previous_order_total,
        LEAD(o.order_date, 1) OVER (
            PARTITION BY c.customer_id
            ORDER BY o.order_date
        ) AS next_order_date
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status IN ('completed', 'shipped')
),

-- Aggregate metrics per customer
customer_summary AS (
    SELECT
        customer_id,
        customer_name,
        email,
        MAX(total_orders) AS lifetime_orders,
        MAX(cumulative_spend) AS lifetime_value,
        MAX(avg_order_value) AS avg_order_value,
        MIN(first_order_date) AS first_purchase_date,
        MAX(last_order_date) AS last_purchase_date,
        -- Apply custom UDF for tier classification
        classify_customer_tier(MAX(cumulative_spend), MAX(total_orders)) AS customer_tier
    FROM customer_order_metrics
    GROUP BY customer_id, customer_name, email
),

-- High value customers from direct orders
high_value_direct AS (
    SELECT
        customer_id,
        customer_name,
        email,
        lifetime_value,
        customer_tier,
        'direct' AS acquisition_channel
    FROM customer_summary
    WHERE lifetime_value > 10000
      AND customer_tier IN ('gold', 'platinum')
),

-- High value customers from partner referrals
high_value_partners AS (
    SELECT
        pr.customer_id,
        c.customer_name,
        c.email,
        SUM(pr.referral_value) AS lifetime_value,
        classify_customer_tier(SUM(pr.referral_value), COUNT(pr.referral_id)) AS customer_tier,
        'partner' AS acquisition_channel
    FROM partner_referrals pr
    INNER JOIN customers c ON pr.customer_id = c.customer_id
    GROUP BY pr.customer_id, c.customer_name, c.email
    HAVING SUM(pr.referral_value) > 10000
),

-- UNION ALL to combine both channels
all_high_value_customers AS (
    SELECT * FROM high_value_direct
    UNION ALL
    SELECT * FROM high_value_partners
    UNION
    -- Also include VIP customers from legacy system
    SELECT
        legacy_id AS customer_id,
        full_name AS customer_name,
        contact_email AS email,
        total_revenue AS lifetime_value,
        membership_level AS customer_tier,
        'legacy' AS acquisition_channel
    FROM legacy_vip_customers
    WHERE is_active = true
),

-- Final ranked results
ranked_customers AS (
    SELECT
        customer_id,
        customer_name,
        email,
        lifetime_value,
        customer_tier,
        acquisition_channel,
        PERCENT_RANK() OVER (ORDER BY lifetime_value DESC) AS value_percentile,
        NTILE(10) OVER (ORDER BY lifetime_value DESC) AS value_decile,
        CUME_DIST() OVER (ORDER BY lifetime_value) AS cumulative_distribution
    FROM all_high_value_customers
)

SELECT * FROM ranked_customers;

-- MERGE statement to upsert customer analytics into target table
MERGE INTO analytics.customer_lifetime_value AS target
USING (
    SELECT
        cs.customer_id,
        cs.customer_name,
        cs.email,
        cs.lifetime_orders,
        cs.lifetime_value,
        cs.avg_order_value,
        cs.first_purchase_date,
        cs.last_purchase_date,
        cs.customer_tier,
        CURRENT_TIMESTAMP() AS updated_at
    FROM customer_summary cs
    WHERE cs.lifetime_orders >= 1
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.lifetime_value <> target.lifetime_value THEN
    UPDATE SET
        target.customer_name = source.customer_name,
        target.email = source.email,
        target.lifetime_orders = source.lifetime_orders,
        target.lifetime_value = source.lifetime_value,
        target.avg_order_value = source.avg_order_value,
        target.first_purchase_date = source.first_purchase_date,
        target.last_purchase_date = source.last_purchase_date,
        target.customer_tier = source.customer_tier,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        customer_name,
        email,
        lifetime_orders,
        lifetime_value,
        avg_order_value,
        first_purchase_date,
        last_purchase_date,
        customer_tier,
        updated_at
    )
    VALUES (
        source.customer_id,
        source.customer_name,
        source.email,
        source.lifetime_orders,
        source.lifetime_value,
        source.avg_order_value,
        source.first_purchase_date,
        source.last_purchase_date,
        source.customer_tier,
        source.updated_at
    )
WHEN NOT MATCHED BY SOURCE AND target.updated_at < DATE_SUB(CURRENT_DATE(), 365) THEN
    DELETE;
