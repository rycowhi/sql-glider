-- Order fact table with dimension keys
-- Populated by: business/load_fact_orders.sql (full refresh)
-- Updated by: incremental/incr_fact_orders.sql (incremental)
CREATE TABLE IF NOT EXISTS fact_orders (
    order_fact_id BIGINT COMMENT 'Surrogate key for order fact',
    order_id BIGINT COMMENT 'Natural key - order identifier',
    order_item_id BIGINT COMMENT 'Natural key - order item identifier',

    -- Dimension keys
    customer_sk BIGINT COMMENT 'Foreign key to dim_customer',
    product_sk BIGINT COMMENT 'Foreign key to dim_product',
    order_date_key INT COMMENT 'Date key (YYYYMMDD format)',

    -- Degenerate dimensions (not in separate dimension tables)
    order_status STRING COMMENT 'Order status',
    order_channel STRING COMMENT 'Order channel',
    line_number INT COMMENT 'Line item sequence number',

    -- Order measures
    quantity INT COMMENT 'Quantity ordered',
    unit_price DECIMAL(10, 2) COMMENT 'Unit price at order time',
    discount_percent DECIMAL(5, 2) COMMENT 'Discount percentage',
    discount_amount DECIMAL(10, 2) COMMENT 'Discount amount',
    tax_amount DECIMAL(10, 2) COMMENT 'Tax amount',
    line_total DECIMAL(10, 2) COMMENT 'Line total amount',

    -- Return measures
    return_flag BOOLEAN COMMENT 'Item returned flag',
    return_date DATE COMMENT 'Return date',
    return_quantity INT COMMENT 'Quantity returned (same as quantity if returned)',
    return_amount DECIMAL(10, 2) COMMENT 'Return amount',

    -- Calculated measures
    net_quantity INT COMMENT 'Quantity minus returns',
    net_amount DECIMAL(10, 2) COMMENT 'Line total minus returns',
    is_first_order BOOLEAN COMMENT 'Customer first order flag',

    -- Order header measures (duplicated per line for easy aggregation)
    order_subtotal DECIMAL(10, 2) COMMENT 'Order subtotal',
    order_tax DECIMAL(10, 2) COMMENT 'Order tax',
    order_shipping DECIMAL(10, 2) COMMENT 'Order shipping fee',
    order_total DECIMAL(10, 2) COMMENT 'Order total',

    -- Metadata
    currency STRING COMMENT 'Currency code',
    order_timestamp TIMESTAMP COMMENT 'Order placement timestamp',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Order fact table with grain: one row per order line item'
PARTITIONED BY (order_date_key);
