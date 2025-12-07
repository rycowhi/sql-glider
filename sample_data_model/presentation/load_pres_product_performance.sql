-- Build product performance metrics
-- Target: pres_product_performance
-- Sources: fact_orders, dim_product
-- Pattern: MERGE (upsert for incremental updates)

MERGE INTO pres_product_performance AS target
USING (
    WITH product_sales AS (
        -- Aggregate sales metrics per product
        SELECT
            dp.product_sk,
            dp.product_id,
            dp.product_name,
            dp.category,
            dp.subcategory,
            dp.brand,
            dp.unit_price AS current_unit_price,
            dp.cost_price AS current_cost_price,

            -- Sales volume
            SUM(fo.net_quantity) AS total_units_sold,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            COUNT(DISTINCT fo.customer_sk) AS unique_customers,

            -- Revenue metrics
            SUM(fo.net_amount) AS total_revenue,
            AVG(fo.line_total) AS avg_line_revenue,
            SUM(fo.discount_amount) AS total_discounts,

            -- Return metrics
            SUM(fo.return_quantity) AS total_units_returned,
            SUM(fo.return_amount) AS total_return_revenue,
            SUM(CASE WHEN fo.return_flag = TRUE THEN 1 ELSE 0 END) AS return_order_count,

            -- Dates
            MIN(DATE(fo.order_timestamp)) AS first_sale_date,
            MAX(DATE(fo.order_timestamp)) AS last_sale_date

        FROM fact_orders fo
        INNER JOIN dim_product dp
            ON fo.product_sk = dp.product_sk

        WHERE fo.order_status IN ('confirmed', 'shipped', 'delivered')

        GROUP BY
            dp.product_sk,
            dp.product_id,
            dp.product_name,
            dp.category,
            dp.subcategory,
            dp.brand,
            dp.unit_price,
            dp.cost_price
    ),
    product_rankings AS (
        -- Rank products within category
        SELECT
            ps.*,
            ROW_NUMBER() OVER (PARTITION BY ps.category ORDER BY ps.total_revenue DESC) AS revenue_rank_in_category,
            ROW_NUMBER() OVER (ORDER BY ps.total_revenue DESC) AS revenue_rank_overall
        FROM product_sales ps
    )
    SELECT
        pr.product_sk,
        pr.product_id,
        pr.product_name,
        pr.category,
        pr.subcategory,
        pr.brand,

        -- Current pricing
        pr.current_unit_price,
        pr.current_cost_price,
        ROUND(((pr.current_unit_price - pr.current_cost_price) / NULLIF(pr.current_unit_price, 0)) * 100, 2) AS margin_percent,

        -- Sales volume
        pr.total_units_sold,
        pr.total_orders,
        pr.unique_customers,
        ROUND(pr.total_units_sold / NULLIF(pr.total_orders, 0), 2) AS avg_units_per_order,

        -- Revenue metrics
        pr.total_revenue,
        pr.avg_line_revenue,
        pr.total_discounts,
        pr.total_revenue - (pr.total_units_sold * pr.current_cost_price) AS total_profit,

        -- Return metrics
        pr.total_units_returned,
        pr.total_return_revenue,
        pr.return_order_count,
        CASE
            WHEN pr.total_units_sold > 0
            THEN ROUND((pr.total_units_returned / pr.total_units_sold) * 100, 2)
            ELSE 0.00
        END AS return_rate_percent,

        -- Performance metrics
        CASE
            WHEN DATEDIFF(pr.last_sale_date, pr.first_sale_date) > 0
            THEN ROUND(pr.total_units_sold / DATEDIFF(pr.last_sale_date, pr.first_sale_date), 2)
            ELSE 0.00
        END AS avg_daily_sales,

        -- Rankings
        pr.revenue_rank_in_category,
        pr.revenue_rank_overall,

        -- Dates
        pr.first_sale_date,
        pr.last_sale_date,
        DATEDIFF(CURRENT_DATE(), pr.last_sale_date) AS days_since_last_sale,

        -- Metadata
        CURRENT_TIMESTAMP() AS load_timestamp

    FROM product_rankings pr
) AS source
ON target.product_sk = source.product_sk
WHEN MATCHED THEN
    UPDATE SET
        product_name = source.product_name,
        category = source.category,
        subcategory = source.subcategory,
        brand = source.brand,
        current_unit_price = source.current_unit_price,
        current_cost_price = source.current_cost_price,
        margin_percent = source.margin_percent,
        total_units_sold = source.total_units_sold,
        total_orders = source.total_orders,
        unique_customers = source.unique_customers,
        avg_units_per_order = source.avg_units_per_order,
        total_revenue = source.total_revenue,
        avg_line_revenue = source.avg_line_revenue,
        total_discounts = source.total_discounts,
        total_profit = source.total_profit,
        total_units_returned = source.total_units_returned,
        total_return_revenue = source.total_return_revenue,
        return_order_count = source.return_order_count,
        return_rate_percent = source.return_rate_percent,
        avg_daily_sales = source.avg_daily_sales,
        revenue_rank_in_category = source.revenue_rank_in_category,
        revenue_rank_overall = source.revenue_rank_overall,
        first_sale_date = source.first_sale_date,
        last_sale_date = source.last_sale_date,
        days_since_last_sale = source.days_since_last_sale,
        load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
    INSERT *;
