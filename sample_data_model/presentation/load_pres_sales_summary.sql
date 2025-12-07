-- Build sales summary with date and category aggregations
-- Target: pres_sales_summary
-- Sources: fact_orders, dim_product, dim_customer
-- Pattern: INSERT OVERWRITE with aggregations

WITH daily_sales AS (
    -- Daily sales metrics
    SELECT
        fo.order_date_key,
        DATE(fo.order_timestamp) AS order_date,
        dp.category AS product_category,
        dc.customer_segment,

        -- Order counts
        COUNT(DISTINCT fo.order_id) AS order_count,
        COUNT(DISTINCT fo.customer_sk) AS unique_customers,
        COUNT(DISTINCT CASE WHEN fo.is_first_order = TRUE THEN fo.customer_sk END) AS new_customers,

        -- Revenue metrics
        SUM(fo.line_total) AS gross_revenue,
        SUM(fo.discount_amount) AS total_discounts,
        SUM(fo.tax_amount) AS total_tax,
        SUM(fo.net_amount) AS net_revenue,
        AVG(fo.line_total) AS avg_line_value,

        -- Product metrics
        SUM(fo.net_quantity) AS units_sold,
        SUM(fo.return_quantity) AS units_returned,
        SUM(fo.return_amount) AS return_revenue,

        -- Channel metrics
        COUNT(DISTINCT fo.order_channel) AS channel_count

    FROM fact_orders fo
    INNER JOIN dim_product dp
        ON fo.product_sk = dp.product_sk
    INNER JOIN dim_customer dc
        ON fo.customer_sk = dc.customer_sk

    WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')

    GROUP BY
        fo.order_date_key,
        DATE(fo.order_timestamp),
        dp.category,
        dc.customer_segment
)
INSERT OVERWRITE TABLE pres_sales_summary
SELECT
    -- Date dimensions
    ds.order_date_key,
    ds.order_date,
    YEAR(ds.order_date) AS order_year,
    MONTH(ds.order_date) AS order_month,
    QUARTER(ds.order_date) AS order_quarter,
    DAYOFWEEK(ds.order_date) AS day_of_week,
    DATE_FORMAT(ds.order_date, 'MMMM') AS month_name,
    DATE_FORMAT(ds.order_date, 'EEEE') AS day_name,

    -- Grouping dimensions
    ds.product_category,
    ds.customer_segment,

    -- Order metrics
    ds.order_count,
    ds.unique_customers,
    ds.new_customers,
    ROUND(ds.order_count / NULLIF(ds.unique_customers, 0), 2) AS orders_per_customer,

    -- Revenue metrics
    ds.gross_revenue,
    ds.total_discounts,
    ds.total_tax,
    ds.net_revenue,
    ds.avg_line_value,
    ROUND(ds.net_revenue / NULLIF(ds.order_count, 0), 2) AS avg_order_value,

    -- Product metrics
    ds.units_sold,
    ds.units_returned,
    ds.return_revenue,
    CASE
        WHEN ds.units_sold > 0
        THEN ROUND((ds.units_returned / ds.units_sold) * 100, 2)
        ELSE 0.00
    END AS return_rate_percent,

    -- Discount analysis
    CASE
        WHEN ds.gross_revenue > 0
        THEN ROUND((ds.total_discounts / ds.gross_revenue) * 100, 2)
        ELSE 0.00
    END AS discount_rate_percent,

    -- YoY comparison placeholders (would use LAG/LEAD in real implementation)
    NULL AS prior_year_revenue,
    NULL AS yoy_revenue_growth_percent,

    -- Metadata
    ds.channel_count,
    CURRENT_TIMESTAMP() AS load_timestamp

FROM daily_sales ds;
