# Plan: Comprehensive Sample Data Model for SQL Glider Testing

**Status:** Completed

**Date:** 2025-12-07

## Overview

Create a fully modeled SQL domain with 22 DML statements (plus 14 DDL files) that work together in a tiered manner to build 4 presentation layer datasets. The model follows a stereotypical customers and orders e-commerce domain, using SparkSQL with MERGE support.

**Location:** `sample_data_model/` in project root

## Data Lineage Flow (Visual)

```
TIER 1: RAW                TIER 2: STAGING           TIER 3: BUSINESS          TIER 4: PRESENTATION
─────────────────────────────────────────────────────────────────v────────────────────────────────────
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

## Data Model Architecture

### Tier 1: Raw/Bronze Layer (Source Tables)
DDL for raw data landing tables - these are the ultimate source tables.

### Tier 2: Staging/Silver Layer
Clean and transform raw data using INSERTs and INSERT OVERWRITEs.

### Tier 3: Business/Gold Layer
Build business entities using complex MERGEs with UPDATE SETs and conditional DELETEs.

### Tier 4: Presentation Layer (4 Final Datasets)
Final aggregated/denormalized datasets for consumption.

## Table Inventory

### Raw Tables (6 tables)
1. `raw_customers` - Raw customer data from source system
2. `raw_addresses` - Raw customer address data
3. `raw_products` - Raw product catalog
4. `raw_orders` - Raw order headers
5. `raw_order_items` - Raw order line items
6. `raw_payments` - Raw payment transactions

### Staging Tables (4 tables)
7. `stg_customers` - Cleaned customer data
8. `stg_products` - Cleaned product data
9. `stg_orders` - Cleaned order data with items joined
10. `stg_payments` - Cleaned and validated payments

### Business Tables (4 tables)
11. `dim_customer` - Customer dimension (SCD Type 2)
12. `dim_product` - Product dimension
13. `fact_orders` - Order fact table
14. `fact_payments` - Payment fact table

### Presentation Tables (4 tables)
15. `pres_customer_360` - Complete customer profile with aggregates
16. `pres_sales_summary` - Daily/monthly sales aggregations
17. `pres_product_performance` - Product sales and metrics
18. `pres_customer_cohort_analysis` - Customer cohort retention analysis

## SQL Statements (22 total)

### DDL Statements (14 files)
| # | File | Description |
|---|------|-------------|
| 1 | `ddl/raw_customers.sql` | DDL for raw_customers |
| 2 | `ddl/raw_addresses.sql` | DDL for raw_addresses |
| 3 | `ddl/raw_products.sql` | DDL for raw_products |
| 4 | `ddl/raw_orders.sql` | DDL for raw_orders |
| 5 | `ddl/raw_order_items.sql` | DDL for raw_order_items |
| 6 | `ddl/raw_payments.sql` | DDL for raw_payments |
| 7 | `ddl/stg_customers.sql` | DDL for stg_customers |
| 8 | `ddl/stg_products.sql` | DDL for stg_products |
| 9 | `ddl/stg_orders.sql` | DDL for stg_orders |
| 10 | `ddl/stg_payments.sql` | DDL for stg_payments |
| 11 | `ddl/dim_customer.sql` | DDL for dim_customer (SCD Type 2) |
| 12 | `ddl/dim_product.sql` | DDL for dim_product |
| 13 | `ddl/fact_orders.sql` | DDL for fact_orders |
| 14 | `ddl/fact_payments.sql` | DDL for fact_payments |

### DML Statements (22 files)
| # | File | Type | Description |
|---|------|------|-------------|
| 1 | `staging/load_stg_customers.sql` | INSERT OVERWRITE | Load and clean customer data |
| 2 | `staging/load_stg_products.sql` | INSERT OVERWRITE | Load and clean product data |
| 3 | `staging/load_stg_orders.sql` | INSERT OVERWRITE | Join orders with items, clean data |
| 4 | `staging/load_stg_payments.sql` | INSERT OVERWRITE | Validate and clean payment data |
| 5 | `business/merge_dim_customer.sql` | MERGE (complex) | SCD Type 2 customer dimension with UPDATE/INSERT |
| 6 | `business/merge_dim_product.sql` | MERGE (simple) | Product dimension upsert |
| 7 | `business/load_fact_orders.sql` | INSERT OVERWRITE | Load order fact with dimension lookups |
| 8 | `business/load_fact_payments.sql` | INSERT OVERWRITE | Load payment fact with dimension lookups |
| 9 | `business/update_dim_customer_metrics.sql` | MERGE (UPDATE only) | Update customer aggregate metrics |
| 10 | `business/expire_dim_customer.sql` | MERGE (UPDATE only) | Expire soft-deleted customers |
| 11 | `presentation/load_pres_customer_360.sql` | INSERT OVERWRITE | Build customer 360 view |
| 12 | `presentation/load_pres_sales_summary.sql` | INSERT OVERWRITE | Build sales summary |
| 13 | `presentation/load_pres_product_performance.sql` | MERGE | Update product performance metrics |
| 14 | `presentation/load_pres_customer_cohort.sql` | INSERT OVERWRITE | Build cohort analysis |
| 15 | `incremental/incr_fact_orders.sql` | MERGE | Incremental order loading |
| 16 | `incremental/incr_fact_payments.sql` | MERGE | Incremental payment loading |
| 17 | `incremental/incr_pres_sales_summary.sql` | MERGE | Incremental sales summary update |
| 18 | `maintenance/delete_expired_customers.sql` | DELETE | Hard delete expired records |
| 19 | `maintenance/update_product_status.sql` | UPDATE | Batch status updates |
| 20 | `complex/multi_table_transform.sql` | Multiple statements | Complex multi-statement transformation |
| 21 | `complex/conditional_merge.sql` | MERGE (complex conditions) | Complex conditional logic in MERGE |
| 22 | `complex/cte_insert.sql` | INSERT with CTEs | Complex CTE-based insert |

## File Structure

```
sample_data_model/
├── README.md                           # Documentation for the data model
├── ddl/                                # Table definitions
│   ├── raw_customers.sql
│   ├── raw_addresses.sql
│   ├── raw_products.sql
│   ├── raw_orders.sql
│   ├── raw_order_items.sql
│   ├── raw_payments.sql
│   ├── stg_customers.sql
│   ├── stg_products.sql
│   ├── stg_orders.sql
│   ├── stg_payments.sql
│   ├── dim_customer.sql
│   ├── dim_product.sql
│   ├── fact_orders.sql
│   └── fact_payments.sql
├── staging/                            # Staging layer transforms
│   ├── load_stg_customers.sql
│   ├── load_stg_products.sql
│   ├── load_stg_orders.sql
│   └── load_stg_payments.sql
├── business/                           # Business layer transforms
│   ├── merge_dim_customer.sql
│   ├── merge_dim_product.sql
│   ├── load_fact_orders.sql
│   ├── load_fact_payments.sql
│   ├── update_dim_customer_metrics.sql
│   └── expire_dim_customer.sql
├── presentation/                       # Presentation layer
│   ├── load_pres_customer_360.sql
│   ├── load_pres_sales_summary.sql
│   ├── load_pres_product_performance.sql
│   └── load_pres_customer_cohort.sql
├── incremental/                        # Incremental load patterns
│   ├── incr_fact_orders.sql
│   ├── incr_fact_payments.sql
│   └── incr_pres_sales_summary.sql
├── maintenance/                        # Maintenance operations
│   ├── delete_expired_customers.sql
│   └── update_product_status.sql
└── complex/                            # Complex multi-statement examples
    ├── multi_table_transform.sql
    ├── conditional_merge.sql
    └── cte_insert.sql
```

## DML Statement Types Distribution

- **INSERT OVERWRITE**: 8 statements (staging loads, fact loads, presentation builds)
- **MERGE (simple)**: 4 statements (basic upserts)
- **MERGE (complex with UPDATE SET)**: 4 statements (SCD Type 2, conditional updates)
- **MERGE (with conditional DELETE)**: 2 statements (expire logic)
- **DELETE**: 1 statement (hard delete maintenance)
- **UPDATE**: 1 statement (batch updates)
- **Multi-statement files**: 2 files (complex transformations)

## Implementation Steps

- [x] Create `sample_data_model/` directory structure
- [x] Create README.md with data model documentation
- [x] Create all 14 DDL files for table definitions
- [x] Create 4 staging layer DML files
- [x] Create 6 business layer DML files
- [x] Create 4 presentation layer DML files
- [x] Create 3 incremental load pattern files
- [x] Create 2 maintenance operation files
- [x] Create 3 complex multi-statement files

## Testing Value

This sample data model provides:
- Multi-tier lineage tracing (4 tiers)
- Complex column-level dependencies
- Various DML statement types for parser testing
- Cross-file relationship testing
- MERGE statement edge cases
- Real-world SQL patterns

## Files Created

Total files: 36
- 1 README.md
- 14 DDL files (but note: presentation layer DDL files were not created as they're typically views or materialized from DML)
- 22 DML files

## Implementation Notes

All 36 SQL files have been successfully created in the `sample_data_model/` directory. The data model demonstrates:

1. **4-tier medallion architecture** (Raw → Staging → Business → Presentation)
2. **Various DML patterns**: INSERT OVERWRITE, MERGE (simple/complex), UPDATE, DELETE
3. **SCD Type 2** implementation for customer dimension
4. **Complex CTEs** with multi-level nesting
5. **Window functions** and analytical queries
6. **Incremental loading patterns** for fact tables
7. **Multi-statement transformations** in single files

All SQL uses SparkSQL syntax with MERGE support as requested.
