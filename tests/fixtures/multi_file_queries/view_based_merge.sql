-- Multi-view pipeline that creates views, then joins and unions them for a MERGE
-- SparkSQL dialect
-- Tests: CREATE VIEW (3x), JOIN, UNION, MERGE

-- View 1: Active customers with their contact info
CREATE OR REPLACE VIEW analytics.v_active_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.created_at,
    a.street_address,
    a.city,
    a.state,
    a.postal_code,
    a.country
FROM customers c
LEFT JOIN addresses a ON c.customer_id = a.customer_id AND a.is_primary = true
WHERE c.status = 'active'
  AND c.created_at >= DATE_SUB(CURRENT_DATE(), 365 * 3);

-- View 2: Customer order statistics
CREATE OR REPLACE VIEW analytics.v_customer_order_stats AS
SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    COUNT(DISTINCT CASE WHEN o.order_date >= DATE_SUB(CURRENT_DATE(), 90) THEN o.order_id END) AS orders_last_90_days,
    SUM(CASE WHEN o.order_date >= DATE_SUB(CURRENT_DATE(), 90) THEN o.order_total ELSE 0 END) AS revenue_last_90_days,
    DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS days_since_last_order
FROM orders o
WHERE o.status IN ('completed', 'shipped', 'delivered')
GROUP BY o.customer_id;

-- View 3: Customer support interactions
CREATE OR REPLACE VIEW analytics.v_customer_support AS
SELECT
    t.customer_id,
    COUNT(DISTINCT t.ticket_id) AS total_tickets,
    COUNT(DISTINCT CASE WHEN t.priority = 'high' THEN t.ticket_id END) AS high_priority_tickets,
    AVG(t.resolution_time_hours) AS avg_resolution_hours,
    SUM(CASE WHEN t.satisfaction_score IS NOT NULL THEN t.satisfaction_score ELSE 0 END) /
        NULLIF(COUNT(CASE WHEN t.satisfaction_score IS NOT NULL THEN 1 END), 0) AS avg_satisfaction_score,
    MAX(t.created_at) AS last_ticket_date,
    COUNT(DISTINCT CASE WHEN t.status = 'open' THEN t.ticket_id END) AS open_tickets
FROM support_tickets t
GROUP BY t.customer_id;

-- Main query: Join two views and union with derived data, then MERGE into target
MERGE INTO analytics.customer_360_view AS target
USING (
    -- Primary dataset: Join active customers with their order stats
    SELECT
        ac.customer_id,
        ac.customer_name,
        ac.email,
        ac.phone,
        ac.city,
        ac.state,
        ac.country,
        COALESCE(os.total_orders, 0) AS total_orders,
        COALESCE(os.total_revenue, 0) AS total_revenue,
        COALESCE(os.avg_order_value, 0) AS avg_order_value,
        os.first_order_date,
        os.last_order_date,
        COALESCE(os.orders_last_90_days, 0) AS orders_last_90_days,
        COALESCE(os.revenue_last_90_days, 0) AS revenue_last_90_days,
        COALESCE(os.days_since_last_order, 9999) AS days_since_last_order,
        COALESCE(cs.total_tickets, 0) AS total_support_tickets,
        COALESCE(cs.avg_satisfaction_score, 0) AS avg_satisfaction_score,
        COALESCE(cs.open_tickets, 0) AS open_support_tickets,
        CASE
            WHEN os.total_revenue >= 100000 THEN 'enterprise'
            WHEN os.total_revenue >= 25000 THEN 'business'
            WHEN os.total_revenue >= 5000 THEN 'professional'
            WHEN os.total_revenue >= 1000 THEN 'starter'
            ELSE 'free'
        END AS customer_segment,
        CASE
            WHEN os.days_since_last_order <= 30 THEN 'active'
            WHEN os.days_since_last_order <= 90 THEN 'at_risk'
            WHEN os.days_since_last_order <= 180 THEN 'dormant'
            ELSE 'churned'
        END AS engagement_status,
        'joined_views' AS data_source,
        CURRENT_TIMESTAMP() AS updated_at
    FROM analytics.v_active_customers ac
    LEFT JOIN analytics.v_customer_order_stats os ON ac.customer_id = os.customer_id
    LEFT JOIN analytics.v_customer_support cs ON ac.customer_id = cs.customer_id

    UNION ALL

    -- Secondary dataset: High-value prospects from marketing leads
    SELECT
        ml.lead_id AS customer_id,
        ml.full_name AS customer_name,
        ml.email,
        ml.phone,
        ml.city,
        ml.state,
        ml.country,
        0 AS total_orders,
        0 AS total_revenue,
        0 AS avg_order_value,
        NULL AS first_order_date,
        NULL AS last_order_date,
        0 AS orders_last_90_days,
        0 AS revenue_last_90_days,
        9999 AS days_since_last_order,
        0 AS total_support_tickets,
        0 AS avg_satisfaction_score,
        0 AS open_support_tickets,
        'prospect' AS customer_segment,
        'new_lead' AS engagement_status,
        'marketing_leads' AS data_source,
        CURRENT_TIMESTAMP() AS updated_at
    FROM marketing_leads ml
    WHERE ml.lead_score >= 80
      AND ml.status = 'qualified'
      AND ml.converted_at IS NULL

    UNION

    -- Tertiary dataset: Reactivation candidates from churned customers
    SELECT
        cc.customer_id,
        cc.customer_name,
        cc.email,
        cc.phone,
        cc.last_known_city AS city,
        cc.last_known_state AS state,
        cc.last_known_country AS country,
        cc.historical_orders AS total_orders,
        cc.historical_revenue AS total_revenue,
        cc.historical_revenue / NULLIF(cc.historical_orders, 0) AS avg_order_value,
        cc.first_order_date,
        cc.last_order_date,
        0 AS orders_last_90_days,
        0 AS revenue_last_90_days,
        DATEDIFF(CURRENT_DATE(), cc.last_order_date) AS days_since_last_order,
        cc.historical_tickets AS total_support_tickets,
        cc.historical_satisfaction AS avg_satisfaction_score,
        0 AS open_support_tickets,
        'reactivation_target' AS customer_segment,
        'churned' AS engagement_status,
        'churned_customers' AS data_source,
        CURRENT_TIMESTAMP() AS updated_at
    FROM churned_customers cc
    WHERE cc.churn_date >= DATE_SUB(CURRENT_DATE(), 365)
      AND cc.reactivation_eligible = true
      AND cc.do_not_contact = false
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND (
    target.total_revenue <> source.total_revenue
    OR target.engagement_status <> source.engagement_status
    OR target.open_support_tickets <> source.open_support_tickets
) THEN
    UPDATE SET
        target.customer_name = source.customer_name,
        target.email = source.email,
        target.phone = source.phone,
        target.city = source.city,
        target.state = source.state,
        target.country = source.country,
        target.total_orders = source.total_orders,
        target.total_revenue = source.total_revenue,
        target.avg_order_value = source.avg_order_value,
        target.first_order_date = COALESCE(target.first_order_date, source.first_order_date),
        target.last_order_date = GREATEST(target.last_order_date, source.last_order_date),
        target.orders_last_90_days = source.orders_last_90_days,
        target.revenue_last_90_days = source.revenue_last_90_days,
        target.days_since_last_order = source.days_since_last_order,
        target.total_support_tickets = source.total_support_tickets,
        target.avg_satisfaction_score = source.avg_satisfaction_score,
        target.open_support_tickets = source.open_support_tickets,
        target.customer_segment = source.customer_segment,
        target.engagement_status = source.engagement_status,
        target.data_source = source.data_source,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        customer_name,
        email,
        phone,
        city,
        state,
        country,
        total_orders,
        total_revenue,
        avg_order_value,
        first_order_date,
        last_order_date,
        orders_last_90_days,
        revenue_last_90_days,
        days_since_last_order,
        total_support_tickets,
        avg_satisfaction_score,
        open_support_tickets,
        customer_segment,
        engagement_status,
        data_source,
        updated_at
    )
    VALUES (
        source.customer_id,
        source.customer_name,
        source.email,
        source.phone,
        source.city,
        source.state,
        source.country,
        source.total_orders,
        source.total_revenue,
        source.avg_order_value,
        source.first_order_date,
        source.last_order_date,
        source.orders_last_90_days,
        source.revenue_last_90_days,
        source.days_since_last_order,
        source.total_support_tickets,
        source.avg_satisfaction_score,
        source.open_support_tickets,
        source.customer_segment,
        source.engagement_status,
        source.data_source,
        source.updated_at
    )
WHEN NOT MATCHED BY SOURCE AND target.updated_at < DATE_SUB(CURRENT_DATE(), 180) THEN
    DELETE;
