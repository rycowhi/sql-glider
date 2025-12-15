# SQL Glider

SQL Utility Toolkit for better understanding, use, and governance of your queries in a native environment.

## Overview

SQL Glider provides powerful column-level and table-level lineage analysis for SQL queries using SQLGlot. It operates on standalone SQL files without requiring a full project setup, making it perfect for ad-hoc analysis, data governance, and understanding query dependencies.

## Features

- **Forward Lineage:** Trace output columns back to their source tables and columns
- **Reverse Lineage:** Impact analysis - find which output columns are affected by a source column
- **Query Dissection:** Decompose SQL into components (CTEs, subqueries, UNION branches) for unit testing
- **Table Extraction:** List all tables in SQL files with usage type (INPUT/OUTPUT) and object type (TABLE/VIEW/CTE)
- **Multi-level Tracing:** Automatically handles CTEs, subqueries, and complex expressions
- **Graph-Based Lineage:** Build and query lineage graphs across thousands of SQL files
- **Multiple Output Formats:** Text (human-readable), JSON (machine-readable), CSV (spreadsheet-ready)
- **Dialect Support:** Works with Spark, PostgreSQL, Snowflake, BigQuery, MySQL, and many more SQL dialects
- **File Export:** Save lineage results to files for documentation or further processing

## Installation

SQL Glider is available on PyPI and can be installed with pip or uv. Python 3.11+ is required.

```bash
# Install with pip
pip install sql-glider

# Or install with uv
uv pip install sql-glider
```

After installation, the `sqlglider` command is available:

```bash
sqlglider lineage query.sql
```

### Development Setup

If you want to contribute or run from source:

```bash
# Clone the repository
git clone https://github.com/ryanholmdahl/sql-glider.git
cd sql-glider

# Install dependencies with uv
uv sync

# Run from source
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
       Query 0: SELECT customer_name, o.order_total FROM customers c JOIN orders o ...
+-----------------------------------------------------------------------------+
| Output Column   | Source Column                                             |
|-----------------+------------------------------------------------------------|
| customer_name   | c.customer_name                                           |
+-----------------------------------------------------------------------------+
Total: 1 row(s)
```

This shows that the output column `customer_name` in Query 0 comes from `c.customer_name` (the `customer_name` column in table `c`).

### Reverse Lineage (Impact Analysis)

Find out which output columns are affected by a source column:

```bash
# Find outputs affected by a source column
uv run sqlglider lineage query.sql --source-column orders.customer_id
```

**Example Output:**
```
          Query 0: SELECT customer_id, segment FROM ...
+---------------------------------------------------------+
| Output Column      | Source Column                      |
|--------------------+------------------------------------|
| orders.customer_id | orders.customer_id                 |
+---------------------------------------------------------+
Total: 1 row(s)
```

This shows that if `orders.customer_id` changes, it will impact the output column `customer_id` in Query 0.

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

### Table Extraction

List all tables involved in SQL files with usage and type information:

```bash
# List all tables in a SQL file
uv run sqlglider tables overview query.sql

# JSON output with detailed table info
uv run sqlglider tables overview query.sql --output-format json

# Export to CSV
uv run sqlglider tables overview query.sql --output-format csv --output-file tables.csv
```

### Pull DDL from Remote Catalogs

Fetch DDL definitions from remote data catalogs (e.g., Databricks Unity Catalog):

```bash
# Pull DDL for all tables used in a SQL file (outputs to stdout)
uv run sqlglider tables pull query.sql --catalog-type databricks

# Save DDL files to a folder (one file per table)
uv run sqlglider tables pull query.sql -c databricks -o ./ddl/

# List available catalog providers
uv run sqlglider tables pull --list
```

**Note:** Requires optional dependencies. Install with: `pip install sql-glider[databricks]`

**Example Output (JSON):**
```json
{
  "queries": [{
    "query_index": 0,
    "tables": [
      {"name": "customers", "usage": "INPUT", "object_type": "UNKNOWN"},
      {"name": "orders", "usage": "INPUT", "object_type": "UNKNOWN"}
    ]
  }]
}
```

**Table Usage Types:**
- `INPUT`: Table is read from (SELECT, JOIN, subqueries)
- `OUTPUT`: Table is written to (INSERT, CREATE TABLE/VIEW, UPDATE)
- `BOTH`: Table is both read from and written to

**Object Types:**
- `TABLE`: CREATE TABLE or DROP TABLE statement
- `VIEW`: CREATE VIEW or DROP VIEW statement
- `CTE`: Common Table Expression (WITH clause)
- `UNKNOWN`: Cannot determine type from SQL alone

### Query Dissection

Decompose SQL queries into constituent parts for unit testing and analysis:

```bash
# Dissect a SQL file (text output)
uv run sqlglider dissect query.sql

# JSON output with full component details
uv run sqlglider dissect query.sql --output-format json

# CSV output for spreadsheet analysis
uv run sqlglider dissect query.sql --output-format csv

# Export to file
uv run sqlglider dissect query.sql -f json -o dissected.json

# With templating support
uv run sqlglider dissect query.sql --templater jinja --var schema=analytics

# From stdin
echo "WITH cte AS (SELECT id FROM users) SELECT * FROM cte" | uv run sqlglider dissect
```

**Example Input:**
```sql
WITH order_totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
)
INSERT INTO analytics.summary
SELECT * FROM order_totals WHERE total > 100
```

**Example Output (JSON):**
```json
{
  "queries": [{
    "query_index": 0,
    "statement_type": "INSERT",
    "total_components": 3,
    "components": [
      {
        "component_type": "CTE",
        "component_index": 0,
        "name": "order_totals",
        "sql": "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        "is_executable": true,
        "dependencies": [],
        "location": "WITH clause"
      },
      {
        "component_type": "TARGET_TABLE",
        "component_index": 1,
        "name": "analytics.summary",
        "sql": "analytics.summary",
        "is_executable": false,
        "location": "INSERT INTO target"
      },
      {
        "component_type": "SOURCE_QUERY",
        "component_index": 2,
        "sql": "SELECT * FROM order_totals WHERE total > 100",
        "is_executable": true,
        "dependencies": ["order_totals"],
        "location": "INSERT source SELECT"
      }
    ]
  }]
}
```

**Extracted Component Types:**
- `CTE`: Common Table Expressions from WITH clause
- `MAIN_QUERY`: The primary SELECT statement
- `SUBQUERY`: Nested SELECT in FROM clause
- `SCALAR_SUBQUERY`: Single-value subquery in SELECT list, WHERE, HAVING
- `TARGET_TABLE`: Output table for INSERT/CREATE/MERGE (not executable)
- `SOURCE_QUERY`: SELECT within DML/DDL statements
- `UNION_BRANCH`: Individual SELECT in UNION/UNION ALL

**Use Cases:**
- Unit test CTEs and subqueries individually
- Extract DQL from CTAS, CREATE VIEW, INSERT statements
- Analyze query structure and component dependencies
- Break apart complex queries for understanding

### Different SQL Dialects

```bash
# PostgreSQL
uv run sqlglider lineage query.sql --dialect postgres

# Snowflake
uv run sqlglider lineage query.sql --dialect snowflake

# BigQuery
uv run sqlglider lineage query.sql --dialect bigquery
```

### Multi-Query Files

SQL Glider automatically detects and analyzes multiple SQL statements in a single file:

```bash
# Analyze all queries in a file
uv run sqlglider lineage multi_query.sql

# Filter to only queries that reference a specific table
uv run sqlglider lineage multi_query.sql --table customers

# Analyze specific column across all queries
uv run sqlglider lineage multi_query.sql --column customer_id

# Reverse lineage across all queries (impact analysis)
uv run sqlglider lineage multi_query.sql --source-column orders.customer_id
```

**Example multi-query file:**
```sql
-- multi_query.sql
SELECT customer_id, customer_name FROM customers;

SELECT order_id, customer_id, order_total FROM orders;

INSERT INTO customer_orders
SELECT c.customer_id, c.customer_name, o.order_id
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;
```

**Output includes query index for each statement:**
```
   Query 0: SELECT customer_id, customer_name FROM customers
+---------------------------------------------------+
| Output Column           | Source Column           |
|-------------------------+-------------------------|
| customers.customer_id   | customers.customer_id   |
| customers.customer_name | customers.customer_name |
+---------------------------------------------------+
Total: 2 row(s)

       Query 1: SELECT order_id, customer_id, order_total FROM orders
+---------------------------------------------+
| Output Column      | Source Column          |
|--------------------+------------------------|
| orders.customer_id | orders.customer_id     |
| orders.order_id    | orders.order_id        |
| orders.order_total | orders.order_total     |
+---------------------------------------------+
Total: 3 row(s)

       Query 2: INSERT INTO customer_orders ...
+---------------------------------------------+
| Output Column      | Source Column          |
|--------------------+------------------------|
...
```

### Graph-Based Lineage (Cross-File Analysis)

For analyzing lineage across multiple SQL files, SQL Glider provides graph commands:

```bash
# Build a lineage graph from a single file
uv run sqlglider graph build query.sql -o graph.json

# Build from multiple files
uv run sqlglider graph build query1.sql query2.sql query3.sql -o graph.json

# Build from a directory (recursively finds all .sql files)
uv run sqlglider graph build ./queries/ -r -o graph.json

# Build from a manifest CSV file
uv run sqlglider graph build --manifest manifest.csv -o graph.json

# Merge multiple graphs into one
uv run sqlglider graph merge graph1.json graph2.json -o merged.json

# Query upstream dependencies (find all sources for a column)
uv run sqlglider graph query graph.json --upstream orders.customer_id

# Query downstream dependencies (find all columns affected by a source)
uv run sqlglider graph query graph.json --downstream customers.id
```

**Example Upstream Query Output:**
```
                              Sources for 'order_totals.total'
+--------------------------------------------------------------------------------------------+
| Column | Table  | Hops | Root | Leaf | Paths                              | File           |
|--------+--------+------+------+------+------------------------------------+----------------|
| amount | orders |    1 |  Y   |  N   | orders.amount -> order_totals.total| test_graph.sql |
+--------------------------------------------------------------------------------------------+

Total: 1 column(s)
```

**Example Downstream Query Output:**
```
                             Affected Columns for 'orders.amount'
+--------------------------------------------------------------------------------------------+
| Column | Table        | Hops | Root | Leaf | Paths                              | File           |
|--------+--------------+------+------+------+------------------------------------+----------------|
| total  | order_totals |    1 |  N   |  Y   | orders.amount -> order_totals.total| test_graph.sql |
+--------------------------------------------------------------------------------------------+

Total: 1 column(s)
```

**Output Fields:**
- **Root**: `Y` if the column has no upstream dependencies (source column)
- **Leaf**: `Y` if the column has no downstream dependencies (final output)
- **Paths**: All paths from the dependency to the queried column

**Manifest File Format:**
```csv
file_path,dialect
queries/orders.sql,spark
queries/customers.sql,postgres
queries/legacy.sql,
```

The graph feature is designed for scale - it can handle thousands of SQL files and provides efficient upstream/downstream queries using rustworkx.

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

### Literal Value Handling

When analyzing UNION queries, SQL Glider identifies literal values (constants) as sources and displays them clearly:

```sql
-- query.sql
SELECT customer_id, last_order_date FROM active_customers
UNION ALL
SELECT customer_id, NULL AS last_order_date FROM prospects
UNION ALL
SELECT customer_id, 'unknown' AS status FROM legacy_data
```

```bash
uv run sqlglider lineage query.sql
```

**Example Output:**
```
       Query 0: SELECT customer_id, last_order_date FROM active_customers ...
+---------------------------------------------------------------------+
| Output Column                    | Source Column                    |
|----------------------------------+----------------------------------|
| active_customers.customer_id     | active_customers.customer_id     |
|                                  | prospects.customer_id            |
| active_customers.last_order_date | <literal: NULL>                  |
|                                  | active_customers.last_order_date |
+---------------------------------------------------------------------+
Total: 4 row(s)
```

Literal values are displayed as `<literal: VALUE>` to clearly distinguish them from actual column sources:
- `<literal: NULL>` - NULL values
- `<literal: 0>` - Numeric literals
- `<literal: 'string'>` - String literals
- `<literal: CURRENT_TIMESTAMP()>` - Function calls

This helps identify which branches of a UNION contribute actual data lineage versus hardcoded values.

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
  --table, -t                 Filter to only queries that reference this table (multi-query files) [optional]
  --output-format, -f         Output format: 'text', 'json', or 'csv' [default: text]
  --output-file, -o           Write output to file instead of stdout [optional]
  --help                      Show help message and exit
```

**Notes:**
- `--column` and `--source-column` are mutually exclusive. Use one or the other.
- `--table` filter is useful for multi-query files to analyze only queries that reference a specific table.

### Tables Command

```
sqlglider tables overview <sql_file> [OPTIONS]

Arguments:
  sql_file                    Path to SQL file to analyze [required]

Options:
  --dialect, -d               SQL dialect (spark, postgres, snowflake, etc.) [default: spark]
  --table                     Filter to only queries that reference this table [optional]
  --output-format, -f         Output format: 'text', 'json', or 'csv' [default: text]
  --output-file, -o           Write output to file instead of stdout [optional]
  --templater, -t             Templater for SQL preprocessing (e.g., 'jinja', 'none') [optional]
  --var, -v                   Template variable in key=value format (repeatable) [optional]
  --vars-file                 Path to variables file (JSON or YAML) [optional]
  --help                      Show help message and exit
```

```
sqlglider tables pull <sql_file> [OPTIONS]

Arguments:
  sql_file                    Path to SQL file to analyze [optional, reads from stdin if omitted]

Options:
  --catalog-type, -c          Catalog provider (e.g., 'databricks') [required if not in config]
  --ddl-folder, -o            Output folder for DDL files [optional, outputs to stdout if omitted]
  --dialect, -d               SQL dialect (spark, postgres, snowflake, etc.) [default: spark]
  --templater, -t             Templater for SQL preprocessing (e.g., 'jinja', 'none') [optional]
  --var, -v                   Template variable in key=value format (repeatable) [optional]
  --vars-file                 Path to variables file (JSON or YAML) [optional]
  --list, -l                  List available catalog providers and exit
  --help                      Show help message and exit
```

**Databricks Setup:**

Install the optional Databricks dependency:
```bash
pip install sql-glider[databricks]
```

Configure authentication (via environment variables or `sqlglider.toml`):
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="abc123..."
```

### Dissect Command

```
sqlglider dissect [sql_file] [OPTIONS]

Arguments:
  sql_file                    Path to SQL file to analyze [optional, reads from stdin if omitted]

Options:
  --dialect, -d               SQL dialect (spark, postgres, snowflake, etc.) [default: spark]
  --output-format, -f         Output format: 'text', 'json', or 'csv' [default: text]
  --output-file, -o           Write output to file instead of stdout [optional]
  --templater, -t             Templater for SQL preprocessing (e.g., 'jinja', 'none') [optional]
  --var, -v                   Template variable in key=value format (repeatable) [optional]
  --vars-file                 Path to variables file (JSON or YAML) [optional]
  --help                      Show help message and exit
```

**Output Fields:**
- `component_type`: Type of component (CTE, MAIN_QUERY, SUBQUERY, etc.)
- `component_index`: Sequential order within the query (0-based)
- `name`: CTE name, subquery alias, or target table name
- `sql`: The extracted SQL for this component
- `is_executable`: Whether the component can run standalone (TARGET_TABLE is false)
- `dependencies`: List of CTE names this component references
- `location`: Human-readable context (e.g., "WITH clause", "FROM clause")
- `depth`: Nesting level (0 = top-level)
- `parent_index`: Index of parent component for nested components

### Graph Commands

```
sqlglider graph build <paths> [OPTIONS]

Arguments:
  paths                       SQL file(s) or directory to process [optional]

Options:
  --output, -o                Output JSON file path [required]
  --manifest, -m              Path to manifest CSV file [optional]
  --recursive, -r             Recursively search directories [default: True]
  --glob, -g                  Glob pattern for SQL files [default: *.sql]
  --dialect, -d               SQL dialect [default: spark]
  --node-format, -n           Node format: 'qualified' or 'structured' [default: qualified]
```

```
sqlglider graph merge <inputs> [OPTIONS]

Arguments:
  inputs                      JSON graph files to merge [optional]

Options:
  --output, -o                Output file path [required]
  --glob, -g                  Glob pattern for graph files [optional]
```

```
sqlglider graph query <graph_file> [OPTIONS]

Arguments:
  graph_file                  Path to graph JSON file [required]

Options:
  --upstream, -u              Find source columns for this column [optional]
  --downstream, -d            Find affected columns for this source [optional]
  --output-format, -f         Output format: 'text', 'json', or 'csv' [default: text]
```

**Notes:**
- `--upstream` and `--downstream` are mutually exclusive. Use one or the other.
- Graph queries are case-insensitive for column matching.

## Output Formats

### Text Format (Default)

Human-readable Rich table format showing query index and preview:

```
       Query 0: SELECT customer_name FROM customers c ...
+---------------------------------------------------+
| Output Column   | Source Column                   |
|-----------------+---------------------------------|
| customer_name   | c.customer_name                 |
+---------------------------------------------------+
Total: 1 row(s)
```

### JSON Format

Machine-readable structured format with query metadata:

```json
{
  "queries": [
    {
      "query_index": 0,
      "query_preview": "SELECT customer_name FROM customers c ...",
      "level": "column",
      "lineage": [
        {
          "output_name": "customer_name",
          "source_name": "c.customer_name"
        }
      ]
    }
  ]
}
```

### CSV Format

Spreadsheet-ready tabular format with query index:

```csv
query_index,output_column,source_column
0,customer_name,c.customer_name
```

**Note:** Each source column gets its own row. If an output column has multiple sources, there will be multiple rows with the same `query_index` and `output_column`.

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

# Type checking
uv run basedpyright
```

### Project Structure

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical documentation.

```
src/sqlglider/
├── cli.py                    # Typer CLI entry point
├── dissection/
│   ├── analyzer.py           # DissectionAnalyzer for query decomposition
│   ├── formatters.py         # Output formatters (text, JSON, CSV)
│   └── models.py             # ComponentType, SQLComponent, QueryDissectionResult
├── graph/
│   ├── builder.py            # Build graphs from SQL files
│   ├── merge.py              # Merge multiple graphs
│   ├── query.py              # Query upstream/downstream lineage
│   └── models.py             # Graph data models
├── lineage/
│   ├── analyzer.py           # Core lineage analysis using SQLGlot
│   └── formatters.py         # Output formatters (text, JSON, CSV)
└── utils/
    └── file_utils.py         # File I/O utilities
```

## Publishing

SQL Glider is configured for publishing to both TestPyPI and PyPI using `uv`.

### Versioning

SQL Glider uses Git tags for version management via [hatch-vcs](https://github.com/ofek/hatch-vcs). The version is automatically derived from Git:

- **Tagged commits:** Version matches the tag (e.g., `git tag v0.2.0` produces version `0.2.0`)
- **Untagged commits:** Version includes development info (e.g., `0.1.dev18+g7216a59`)

**Creating a new release:**

```bash
# Create and push a version tag
git tag v0.2.0
git push origin v0.2.0

# Build will now produce version 0.2.0
uv build
```

**Tag format:** Use `v` prefix (e.g., `v1.0.0`, `v0.2.1`). The `v` is stripped from the final version number.

### Building the Package

```bash
# Build the distribution files (wheel and sdist)
uv build
```

This creates distribution files in the `dist/` directory.

### Publishing to TestPyPI

Always test your release on TestPyPI first:

```bash
# Publish to TestPyPI
uv publish --index testpypi --token <YOUR_TESTPYPI_TOKEN>

# Test installation from TestPyPI
uv pip install --index-url https://test.pypi.org/simple/ sql-glider
```

### Publishing to PyPI

Once verified on TestPyPI, publish to production:

```bash
# Publish to PyPI
uv publish --index pypi --token <YOUR_PYPI_TOKEN>
```

### Token Setup

You'll need API tokens from both registries:

1. **TestPyPI Token:** Create at https://test.pypi.org/manage/account/token/
2. **PyPI Token:** Create at https://pypi.org/manage/account/token/

**Option 1: Pass token directly (shown above)**

**Option 2: Environment variable**
```bash
export UV_PUBLISH_TOKEN=pypi-...
uv publish --index pypi
```

**Option 3: Store in `.env` file (not committed to git)**
```bash
# .env
UV_PUBLISH_TOKEN=pypi-...
```

**Security Note:** Never commit API tokens to version control. The `.gitignore` file should include `.env`.

## Dependencies

- **sqlglot[rs]:** SQL parser and lineage analysis library with Rust extensions
- **typer:** CLI framework with type hints
- **rich:** Terminal formatting and colored output
- **pydantic:** Data validation and serialization
- **rustworkx:** High-performance graph library for cross-file lineage analysis

## References

- [SQLGlot Documentation](https://sqlglot.com/)
- [UV Documentation](https://docs.astral.sh/uv/)
- [Typer Documentation](https://typer.tiangolo.com/)
- [Ruff Documentation](https://docs.astral.sh/ruff/configuration/)

## License

See LICENSE file for details.
