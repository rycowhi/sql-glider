---
icon: lucide/rocket
---

# SQL Glider

SQL Glider is a SQL Utility Toolkit for better understanding, use, and governance of SQL queries in a native environment. It provides column-level and table-level lineage analysis for standalone SQL files using SQLGlot's powerful parsing and lineage capabilities.

## Installation

```bash
pip install sql-glider
```

## Quick Start

```bash
# Column-level lineage for a SQL file
sqlglider lineage query.sql

# Table-level lineage
sqlglider lineage query.sql --level table

# JSON output
sqlglider lineage query.sql --output-format json

# Pipe SQL from stdin
echo "SELECT id, name FROM users" | sqlglider lineage
```

## Features

- **Column-Level Lineage** - Trace the origin of every output column back to its source tables
- **Table-Level Lineage** - Understand table dependencies at a glance
- **Multi-Query Support** - Analyze files with multiple SQL statements
- **Graph-Based Cross-File Analysis** - Build and query lineage graphs across your entire SQL codebase
- **SQL Templating** - Jinja2 support for parameterized SQL
- **Query Dissection** - Decompose complex queries into testable components
- **Table Extraction** - Extract all tables with usage and type information
- **DDL Retrieval** - Pull DDL definitions from remote data catalogs
- **Multiple Dialects** - Supports Spark, Postgres, Snowflake, BigQuery, and more
