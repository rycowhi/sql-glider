# Sample Data Model for SQL Glider Testing

This directory contains a comprehensive, multi-tiered SQL data model designed to test SQL Glider's lineage analysis capabilities across complex real-world scenarios.

## Overview

**Domain**: E-commerce (Customers & Orders)
**SQL Dialect**: SparkSQL with MERGE support
**Total Files**: 36 SQL files (14 DDL + 22 DML)
**Architecture**: 4-tier medallion architecture (Raw → Staging → Business → Presentation)

## Data Lineage Flow

```
TIER 1: RAW                TIER 2: STAGING           TIER 3: BUSINESS          TIER 4: PRESENTATION
─────────────────────────────────────────────────────────────────────────────────────────────────────
raw_customers ───┐
                 ├──► stg_customers ────┬──► dim_customer ─────┬──► pres_customer_360
raw_addresses ───┘                      │                      │
                                        │                      ├──► pres_customer_cohort_analysis
raw_products ────────► stg_products ────┼──► dim_product ──────┤
                                        │                      ├──► pres_product_performance
raw_orders ──────┐                      │                      │
                 ├──► stg_orders ───────┼──► fact_orders ──────┼──► pres_sales_summary
raw_order_items ─┘                      │                      │
                                        │                      │
raw_payments ────────► stg_payments ────┴──► fact_payments ────┘
```

## Directory Structure

```
sample_data_model/
├── README.md                           # This file
├── ddl/                                # Table definitions (14 files)
│   ├── raw_customers.sql               # Raw customer master data
│   ├── raw_addresses.sql               # Raw customer addresses
│   ├── raw_products.sql                # Raw product catalog
│   ├── raw_orders.sql                  # Raw order headers
│   ├── raw_order_items.sql             # Raw order line items
│   ├── raw_payments.sql                # Raw payment transactions
│   ├── stg_customers.sql               # Staging customer table
│   ├── stg_products.sql                # Staging product table
│   ├── stg_orders.sql                  # Staging orders (denormalized)
│   ├── stg_payments.sql                # Staging payments
│   ├── dim_customer.sql                # Customer dimension (SCD Type 2)
│   ├── dim_product.sql                 # Product dimension
│   ├── fact_orders.sql                 # Order fact table
│   └── fact_payments.sql               # Payment fact table
├── staging/                            # Staging transformations (4 files)
│   ├── load_stg_customers.sql          # Clean customer data
│   ├── load_stg_products.sql           # Clean product data
│   ├── load_stg_orders.sql             # Join orders with items
│   └── load_stg_payments.sql           # Validate payments
├── business/                           # Business layer (6 files)
│   ├── merge_dim_customer.sql          # SCD Type 2 MERGE
│   ├── merge_dim_product.sql           # Simple product upsert
│   ├── load_fact_orders.sql            # Load order facts
│   ├── load_fact_payments.sql          # Load payment facts
│   ├── update_dim_customer_metrics.sql # Update customer metrics
│   └── expire_dim_customer.sql         # Expire deleted customers
├── presentation/                       # Presentation layer (4 files)
│   ├── load_pres_customer_360.sql      # Customer 360 view
│   ├── load_pres_sales_summary.sql     # Sales aggregations
│   ├── load_pres_product_performance.sql # Product metrics
│   └── load_pres_customer_cohort.sql   # Cohort analysis
├── incremental/                        # Incremental patterns (3 files)
│   ├── incr_fact_orders.sql            # Incremental order loading
│   ├── incr_fact_payments.sql          # Incremental payment loading
│   └── incr_pres_sales_summary.sql     # Incremental sales update
├── maintenance/                        # Maintenance operations (2 files)
│   ├── delete_expired_customers.sql    # Hard delete expired records
│   └── update_product_status.sql       # Batch status updates
└── complex/                            # Complex examples (3 files)
    ├── multi_table_transform.sql       # Multi-statement pipeline
    ├── conditional_merge.sql           # Complex MERGE logic
    └── cte_insert.sql                  # CTE-based INSERT

Total: 36 files (1 README + 14 DDL + 22 DML)
```

## DML Statement Types

This model demonstrates various DML patterns:

| Statement Type | Count | Examples |
|---------------|-------|----------|
| INSERT OVERWRITE | 8 | Staging loads, fact loads, presentation builds |
| MERGE (simple) | 4 | Basic upserts with MATCHED/NOT MATCHED |
| MERGE (complex UPDATE SET) | 4 | SCD Type 2, conditional updates |
| MERGE (conditional DELETE) | 2 | Expire logic, soft delete handling |
| DELETE | 1 | Hard delete maintenance |
| UPDATE | 1 | Batch status updates |
| Multi-statement | 2 | Complex transformation pipelines |

## Table Inventory

### Tier 1: Raw Tables (6 tables)
1. **raw_customers** - Customer master data from source system
2. **raw_addresses** - Customer address information
3. **raw_products** - Product catalog
4. **raw_orders** - Order header records
5. **raw_order_items** - Order line items
6. **raw_payments** - Payment transaction records

### Tier 2: Staging Tables (4 tables)
7. **stg_customers** - Cleaned and validated customer data
8. **stg_products** - Cleaned product data
9. **stg_orders** - Denormalized orders (header + items)
10. **stg_payments** - Validated payment data

### Tier 3: Business Tables (4 tables)
11. **dim_customer** - Customer dimension with SCD Type 2 history
12. **dim_product** - Product dimension
13. **fact_orders** - Order fact table with dimension keys
14. **fact_payments** - Payment fact table with dimension keys

### Tier 4: Presentation Tables (4 tables)
15. **pres_customer_360** - Complete customer profile with aggregates
16. **pres_sales_summary** - Sales aggregations by date/category
17. **pres_product_performance** - Product sales and performance metrics
18. **pres_customer_cohort_analysis** - Customer cohort retention analysis

## Presentation Layer Outputs

### 1. pres_customer_360
Complete customer profile including:
- Customer demographics (name, email, phone, address)
- Aggregated order history (total orders, total spent)
- Payment history (total payments, payment methods)
- Customer lifetime value (CLV)
- First/last order dates
- Customer segment classification

### 2. pres_sales_summary
Sales aggregations including:
- Date dimensions (daily, monthly, yearly)
- Product category breakdowns
- Customer segment analysis
- Total revenue, order count, average order value
- Year-over-year comparisons

### 3. pres_product_performance
Product metrics including:
- Total units sold per product
- Revenue by product
- Return rates and return revenue
- Inventory turnover metrics
- Product ranking by revenue
- Top performing categories

### 4. pres_customer_cohort_analysis
Cohort analysis including:
- Acquisition cohort (by month/quarter)
- Retention rates over time
- Revenue per cohort
- Cohort size and growth
- Cohort comparison metrics

## Testing SQL Glider Features

This data model is designed to test:

### Column-Level Lineage
- Multi-hop transformations (4 tiers deep)
- Complex aggregations and calculations
- Window functions and analytical queries
- CASE expressions and conditional logic
- CTEs with multi-level dependencies

### Table-Level Lineage
- Cross-file dependencies
- Multi-source joins
- Denormalization patterns
- Fact-dimension relationships

### DML Patterns
- INSERT OVERWRITE (full table replace)
- MERGE with UPDATE/INSERT (upserts)
- MERGE with DELETE (conditional deletes)
- SCD Type 2 pattern (historical tracking)
- Incremental loading patterns

### Graph Building
- Multi-file graph construction
- Tiered dependency resolution
- Upstream/downstream queries
- Cross-layer lineage tracing

## Usage Examples

### Analyze Single File
```bash
# Column-level lineage for a staging file
uv run sqlglider lineage staging/load_stg_customers.sql

# Table-level lineage for a presentation file
uv run sqlglider lineage presentation/load_pres_customer_360.sql --level table
```

### Build Cross-File Graph
```bash
# Build graph from all files
uv run sqlglider graph build sample_data_model/ -r -o sample_model_graph.json

# Query upstream dependencies
uv run sqlglider graph query sample_model_graph.json --upstream pres_customer_360.customer_name

# Query downstream impact
uv run sqlglider graph query sample_model_graph.json --downstream raw_customers.customer_id
```

### Multi-Query Analysis
```bash
# Analyze complex multi-statement file
uv run sqlglider lineage complex/multi_table_transform.sql --output-format json
```

## Data Model Characteristics

- **Realistic business logic**: Based on common e-commerce patterns
- **Multiple transformation types**: Cleaning, joining, aggregating, calculating
- **SCD Type 2 pattern**: Historical tracking in dim_customer
- **Incremental patterns**: Shows both full refresh and incremental loading
- **Complex dependencies**: Multi-level column transformations
- **Various join types**: INNER, LEFT, FULL OUTER joins
- **Aggregations**: GROUP BY, window functions, analytical queries
- **Data quality**: NULL handling, deduplication, validation logic

## Notes

- All SQL uses **SparkSQL syntax** with MERGE support
- DDL files use standard SparkSQL CREATE TABLE syntax
- Each DML file focuses on a single, specific transformation
- Multi-statement files demonstrate complex pipelines
- All column names follow snake_case convention
- Surrogate keys use `_sk` suffix
- Natural keys use `_id` suffix
