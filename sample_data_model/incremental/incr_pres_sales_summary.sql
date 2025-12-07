-- Incremental update for sales summary
-- Target: pres_sales_summary
-- Sources: fact_orders, dim_product, dim_customer
-- Pattern: MERGE (incremental update for recent dates)

MERGE INTO pres_sales_summary AS target
USING (
    WITH daily_sales AS (
        -- Daily sales metrics for incremental window
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

        WHERE
            fo.order_status IN ('confirmed', 'shipped', 'delivered')
            -- Only process last 30 days (incremental window)
            AND fo.order_date_key >= CAST(DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 30), 'yyyyMMdd') AS INT)

        GROUP BY
            fo.order_date_key,
            DATE(fo.order_timestamp),
            dp.category,
            dc.customer_segment
    )
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

        -- YoY comparison placeholders
        NULL AS prior_year_revenue,
        NULL AS yoy_revenue_growth_percent,

        -- Metadata
        ds.channel_count,
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM daily_sales ds
) AS source
ON target.order_date_key = source.order_date_key
   AND target.product_category = source.product_category
   AND target.customer_segment = source.customer_segment
WHEN MATCHED THEN
    UPDATE SET
        order_count = source.order_count,
        unique_customers = source.unique_customers,
        new_customers = source.new_customers,
        orders_per_customer = source.orders_per_customer,
        gross_revenue = source.gross_revenue,
        total_discounts = source.total_discounts,
        total_tax = source.total_tax,
        net_revenue = source.net_revenue,
        avg_line_value = source.avg_line_value,
        avg_order_value = source.avg_order_value,
        units_sold = source.units_sold,
        units_returned = source.units_returned,
        return_revenue = source.return_revenue,
        return_rate_percent = source.return_rate_percent,
        discount_rate_percent = source.discount_rate_percent,
        channel_count = source.channel_count,
        load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
    INSERT *;
