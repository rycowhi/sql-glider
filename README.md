# SQL Glider

SQL Utility Toolkit for better understanding, use, and governance of your queries in a native environment.

## Overview

SQL Glider provides powerful column-level and table-level lineage analysis for SQL queries using SQLGlot. It operates on standalone SQL files without requiring a full project setup, making it perfect for ad-hoc analysis, data governance, and understanding query dependencies.

## Features

- **Forward Lineage:** Trace output columns back to their source tables and columns
- **Reverse Lineage:** Impact analysis - find which output columns are affected by a source column
- **Multi-level Tracing:** Automatically handles CTEs, subqueries, and complex expressions
- **Multiple Output Formats:** Text (human-readable), JSON (machine-readable), CSV (spreadsheet-ready)
- **Dialect Support:** Works with Spark, PostgreSQL, Snowflake, BigQuery, MySQL, and many more SQL dialects
- **File Export:** Save lineage results to files for documentation or further processing

## Installation

This project uses `uv` for Python package management. Python 3.11+ is required.

```bash
# Install dependencies
uv sync

# Run SQL Glider
uv run sqlglider lineage <sql_file>
```

## Quick Start

### Forward Lineage (Source Tracing)

Find out where your output columns come from:

```bash
# Analyze all output columns
uv run sqlglider lineage query.sql

# Analyze a specific output column
uv run sqlglider lineage query.sql --column customer_name
```

**Example Output:**
```
----------
customer_name
----------
c.customer_name
```

This shows that the output column `customer_name` comes from `c.customer_name` (the `customer_name` column in table `c`).

### Reverse Lineage (Impact Analysis)

Find out which output columns are affected by a source column:

```bash
# Find outputs affected by a source column
uv run sqlglider lineage query.sql --source-column orders.customer_id
```

**Example Output:**
```
----------
orders.customer_id
----------
customer_id
segment
```

This shows that if `orders.customer_id` changes, it will impact the output columns `customer_id` and `segment`.

## Usage Examples

### Basic Column Lineage

```bash
# Forward lineage for all columns
uv run sqlglider lineage query.sql

# Forward lineage for specific column
uv run sqlglider lineage query.sql --column order_total

# Reverse lineage (impact analysis)
uv run sqlglider lineage query.sql --source-column orders.customer_id
```

### Different Output Formats

```bash
# JSON output
uv run sqlglider lineage query.sql --output-format json

# CSV output
uv run sqlglider lineage query.sql --output-format csv

# Export to file
uv run sqlglider lineage query.sql --output-format json --output-file lineage.json
```

### Table-Level Lineage

```bash
# Show which tables are used
uv run sqlglider lineage query.sql --level table
```

### Different SQL Dialects

```bash
# PostgreSQL
uv run sqlglider lineage query.sql --dialect postgres

# Snowflake
uv run sqlglider lineage query.sql --dialect snowflake

# BigQuery
uv run sqlglider lineage query.sql --dialect bigquery
```

## Use Cases

### Data Governance

**Impact Assessment:**
```bash
# Before modifying a source column, check its impact
uv run sqlglider lineage analytics_dashboard.sql --source-column orders.revenue
```

This helps you understand which downstream outputs will be affected by schema changes.

### Query Understanding

**Source Tracing:**
```bash
# Understand where a metric comes from
uv run sqlglider lineage metrics.sql --column total_revenue
```

Quickly trace complex calculations back to their source tables.

### Documentation

**Export Lineage:**
```bash
# Generate documentation for your queries
uv run sqlglider lineage query.sql --output-format csv --output-file docs/lineage.csv
```

Create machine-readable lineage documentation for data catalogs.

### Multi-Level Analysis

SQL Glider automatically traces through CTEs and subqueries:

```sql
-- query.sql
WITH order_totals AS (
    SELECT customer_id, SUM(order_amount) as total_amount
    FROM orders
    GROUP BY customer_id
),
customer_segments AS (
    SELECT
        ot.customer_id,
        c.customer_name,
        CASE
            WHEN ot.total_amount > 10000 THEN 'Premium'
            ELSE 'Standard'
        END as segment
    FROM order_totals ot
    JOIN customers c ON ot.customer_id = c.customer_id
)
SELECT customer_name, segment, total_amount
FROM customer_segments
```

```bash
# Trace segment back to its ultimate sources
uv run sqlglider lineage query.sql --column segment
# Output: orders.order_amount (through the CASE statement and SUM)

# Find what's affected by order_amount
uv run sqlglider lineage query.sql --source-column orders.order_amount
# Output: segment, total_amount
```

## CLI Reference

```
sqlglider lineage <sql_file> [OPTIONS]

Arguments:
  sql_file                    Path to SQL file to analyze [required]

Options:
  --level, -l                 Analysis level: 'column' or 'table' [default: column]
  --dialect, -d               SQL dialect (spark, postgres, snowflake, etc.) [default: spark]
  --column, -c                Specific output column for forward lineage [optional]
  --source-column, -s         Source column for reverse lineage (impact analysis) [optional]
  --output-format, -f         Output format: 'text', 'json', or 'csv' [default: text]
  --output-file, -o           Write output to file instead of stdout [optional]
  --help                      Show help message and exit
```

**Note:** `--column` and `--source-column` are mutually exclusive. Use one or the other.

## Output Formats

### Text Format (Default)

Human-readable format with clear separators:

```
----------
customer_name
----------
c.customer_name
```

### JSON Format

Machine-readable structured format:

```json
{
  "columns": [
    {
      "output_column": "customer_name",
      "source_columns": ["c.customer_name"]
    }
  ]
}
```

### CSV Format

Spreadsheet-ready tabular format:

```csv
output_column,source_table,source_column
customer_name,c,customer_name
```

## Development

### Setup

```bash
# Install dependencies
uv sync

# Run linter
uv run ruff check

# Auto-fix issues
uv run ruff check --fix

# Format code
uv run ruff format
```

### Project Structure

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical documentation.

```
src/sqlglider/
├── cli.py                    # Typer CLI entry point
├── lineage/
│   ├── analyzer.py           # Core lineage analysis using SQLGlot
│   └── formatters.py         # Output formatters (text, JSON, CSV)
└── utils/
    └── file_utils.py         # File I/O utilities
```

## Dependencies

- **sqlglot[rs]:** SQL parser and lineage analysis library with Rust extensions
- **typer:** CLI framework with type hints
- **rich:** Terminal formatting and colored output
- **pydantic:** Data validation and serialization

## References

- [SQLGlot Documentation](https://sqlglot.com/)
- [UV Documentation](https://docs.astral.sh/uv/)
- [Typer Documentation](https://typer.tiangolo.com/)
- [Ruff Documentation](https://docs.astral.sh/ruff/configuration/)

## License

See LICENSE file for details.
