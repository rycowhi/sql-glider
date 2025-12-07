-- Orders fact table
SELECT
    order_id,
    customer_id,
    order_total,
    order_date,
    status
FROM raw_orders;
