-- Raw order line item data
-- Source: Order management system real-time feed
CREATE TABLE IF NOT EXISTS raw_order_items (
    order_item_id BIGINT COMMENT 'Unique order item identifier',
    order_id BIGINT COMMENT 'Foreign key to order header',
    product_id BIGINT COMMENT 'Foreign key to product',
    line_number INT COMMENT 'Line item sequence number',
    quantity INT COMMENT 'Quantity ordered',
    unit_price DECIMAL(10, 2) COMMENT 'Unit price at time of order',
    discount_percent DECIMAL(5, 2) COMMENT 'Discount percentage applied',
    discount_amount DECIMAL(10, 2) COMMENT 'Line item discount amount',
    tax_amount DECIMAL(10, 2) COMMENT 'Line item tax amount',
    line_total DECIMAL(10, 2) COMMENT 'Line item total (qty * unit_price - discount + tax)',
    return_flag BOOLEAN COMMENT 'Item return flag',
    return_date DATE COMMENT 'Item return date (NULL if not returned)',
    return_reason STRING COMMENT 'Return reason code',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw order line item data from order management system'
PARTITIONED BY (order_id);
