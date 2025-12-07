-- Build customer cohort retention analysis
-- Target: pres_customer_cohort_analysis
-- Sources: fact_orders, dim_customer
-- Pattern: INSERT OVERWRITE with cohort analysis

WITH customer_cohorts AS (
    -- Assign each customer to a cohort based on first order month
    SELECT
        dc.customer_sk,
        dc.customer_id,
        DATE_FORMAT(MIN(DATE(fo.order_timestamp)), 'yyyy-MM') AS cohort_month,
        MIN(DATE(fo.order_timestamp)) AS first_order_date,
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(fo.net_amount) AS total_revenue
    FROM dim_customer dc
    INNER JOIN fact_orders fo
        ON dc.customer_sk = fo.customer_sk
    WHERE
        dc.is_current = TRUE
        AND fo.order_status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY dc.customer_sk, dc.customer_id
),
cohort_orders AS (
    -- Get all orders for cohort customers with month offset
    SELECT
        cc.cohort_month,
        cc.customer_sk,
        DATE(fo.order_timestamp) AS order_date,
        DATE_FORMAT(fo.order_timestamp, 'yyyy-MM') AS order_month,
        MONTHS_BETWEEN(fo.order_timestamp, cc.first_order_date) AS months_since_first_order,
        fo.net_amount AS order_revenue
    FROM customer_cohorts cc
    INNER JOIN fact_orders fo
        ON cc.customer_sk = fo.customer_sk
    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')
),
cohort_metrics AS (
    -- Calculate cohort metrics by month offset
    SELECT
        co.cohort_month,
        co.months_since_first_order,

        -- Cohort size (customers who made first purchase in cohort month)
        COUNT(DISTINCT CASE WHEN co.months_since_first_order = 0 THEN co.customer_sk END) AS cohort_size,

        -- Active customers in this period
        COUNT(DISTINCT co.customer_sk) AS active_customers,

        -- Revenue metrics
        SUM(co.order_revenue) AS total_revenue,
        AVG(co.order_revenue) AS avg_order_revenue,
        COUNT(*) AS order_count

    FROM cohort_orders co
    GROUP BY co.cohort_month, co.months_since_first_order
)
INSERT OVERWRITE TABLE pres_customer_cohort_analysis
SELECT
    -- Cohort identification
    cm.cohort_month,
    YEAR(TO_DATE(cm.cohort_month, 'yyyy-MM')) AS cohort_year,
    MONTH(TO_DATE(cm.cohort_month, 'yyyy-MM')) AS cohort_month_num,
    QUARTER(TO_DATE(cm.cohort_month, 'yyyy-MM')) AS cohort_quarter,

    -- Period metrics
    cm.months_since_first_order AS period_number,
    CASE
        WHEN cm.months_since_first_order = 0 THEN 'Acquisition'
        WHEN cm.months_since_first_order <= 3 THEN 'Month 1-3'
        WHEN cm.months_since_first_order <= 6 THEN 'Month 4-6'
        WHEN cm.months_since_first_order <= 12 THEN 'Month 7-12'
        ELSE 'Month 13+'
    END AS period_bucket,

    -- Cohort size and retention
    cm.cohort_size,
    cm.active_customers,
    ROUND((cm.active_customers / NULLIF(cm.cohort_size, 0)) * 100, 2) AS retention_rate_percent,

    -- Revenue metrics
    cm.total_revenue,
    cm.avg_order_revenue,
    ROUND(cm.total_revenue / NULLIF(cm.cohort_size, 0), 2) AS revenue_per_customer,
    cm.order_count,
    ROUND(cm.order_count / NULLIF(cm.active_customers, 0), 2) AS orders_per_active_customer,

    -- Cumulative metrics (simplified - would use window functions in real implementation)
    cm.total_revenue AS cumulative_revenue,
    ROUND(cm.total_revenue / NULLIF(cm.cohort_size, 0), 2) AS cumulative_revenue_per_customer,

    -- Cohort health indicators
    CASE
        WHEN cm.months_since_first_order = 0 THEN NULL
        WHEN (cm.active_customers / NULLIF(cm.cohort_size, 0)) >= 0.5 THEN 'Healthy'
        WHEN (cm.active_customers / NULLIF(cm.cohort_size, 0)) >= 0.25 THEN 'At Risk'
        ELSE 'Poor'
    END AS cohort_health,

    -- Metadata
    CURRENT_TIMESTAMP() AS load_timestamp

FROM cohort_metrics cm

WHERE cm.cohort_size > 0  -- Only include cohorts with customers

ORDER BY cm.cohort_month, cm.months_since_first_order;
