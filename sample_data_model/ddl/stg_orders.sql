-- Staging orders (denormalized order headers + items)
-- Populated by: staging/load_stg_orders.sql
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id BIGINT COMMENT 'Order identifier',
    order_item_id BIGINT COMMENT 'Unique order item identifier',
    customer_id BIGINT COMMENT 'Customer identifier',
    product_id BIGINT COMMENT 'Product identifier',

    -- Order header attributes
    order_date TIMESTAMP COMMENT 'Order placement timestamp',
    order_date_key INT COMMENT 'Date key (YYYYMMDD format)',
    order_status STRING COMMENT 'Order status',
    order_channel STRING COMMENT 'Order channel: web, mobile, phone, store',

    -- Order item attributes
    line_number INT COMMENT 'Line item sequence number',
    quantity INT COMMENT 'Quantity ordered',
    unit_price DECIMAL(10, 2) COMMENT 'Unit price at time of order',
    discount_percent DECIMAL(5, 2) COMMENT 'Discount percentage',
    discount_amount DECIMAL(10, 2) COMMENT 'Line item discount amount',
    tax_amount DECIMAL(10, 2) COMMENT 'Line item tax amount',
    line_total DECIMAL(10, 2) COMMENT 'Line item total',

    -- Return information
    return_flag BOOLEAN COMMENT 'Item return flag',
    return_date DATE COMMENT 'Item return date',
    return_reason STRING COMMENT 'Return reason code',

    -- Order totals (from header, repeated per line)
    order_subtotal DECIMAL(10, 2) COMMENT 'Order subtotal',
    order_tax DECIMAL(10, 2) COMMENT 'Total order tax',
    order_shipping DECIMAL(10, 2) COMMENT 'Order shipping fee',
    order_total DECIMAL(10, 2) COMMENT 'Order total amount',

    -- Calculated fields
    net_line_total DECIMAL(10, 2) COMMENT 'Line total minus returns',
    is_first_order BOOLEAN COMMENT 'Customer first order flag',

    -- Metadata
    currency STRING COMMENT 'Order currency',
    load_timestamp TIMESTAMP COMMENT 'ETL load timestamp'
)
USING DELTA
COMMENT 'Staging table for denormalized order data (header + items)'
PARTITIONED BY (order_date_key);
