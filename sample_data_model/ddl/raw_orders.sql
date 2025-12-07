-- Raw order header data
-- Source: Order management system real-time feed
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id BIGINT COMMENT 'Unique order identifier',
    customer_id BIGINT COMMENT 'Foreign key to customer',
    order_date TIMESTAMP COMMENT 'Order placement timestamp',
    order_status STRING COMMENT 'Order status: pending, confirmed, shipped, delivered, cancelled',
    shipping_address_id BIGINT COMMENT 'Foreign key to shipping address',
    billing_address_id BIGINT COMMENT 'Foreign key to billing address',
    order_channel STRING COMMENT 'Order channel: web, mobile, phone, store',
    currency STRING COMMENT 'Order currency code (ISO 4217)',
    subtotal_amount DECIMAL(10, 2) COMMENT 'Order subtotal before tax and shipping',
    tax_amount DECIMAL(10, 2) COMMENT 'Total tax amount',
    shipping_amount DECIMAL(10, 2) COMMENT 'Shipping fee',
    discount_amount DECIMAL(10, 2) COMMENT 'Total discount amount',
    total_amount DECIMAL(10, 2) COMMENT 'Order total amount',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Raw order header data from order management system'
PARTITIONED BY (DATE(order_date));
