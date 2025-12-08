# Plan: Add "tables" Command to SQL Glider CLI

**Status:** Completed
**Date:** 2025-12-08
**Completed:** 2025-12-08

## Overview

Add a new `tables` command to the SQL Glider CLI that outputs all tables involved in a SQL file, including:
- Fully qualified table names (no aliases)
- Usage type (INPUT, OUTPUT, or BOTH)
- Object type (TABLE, VIEW, CTE, or UNKNOWN)

## Design Decisions (Confirmed with User)

1. **UNKNOWN object type**: Use `"UNKNOWN"` when we can't determine if something is a TABLE or VIEW (e.g., in SELECT statements)
2. **CTEs**: Include CTEs but mark them with object type `"CTE"` to distinguish from database objects
3. **Subqueries**: Include all tables from subqueries (full recursive extraction)
4. **Aliases**: Show only the original fully-qualified table name, not aliases
5. **Multi-query files**: Show tables per query (consistent with existing `lineage` command)

### 1. Data Model

Create new Pydantic models in `src/sqlglider/lineage/analyzer.py`:

```python
class TableUsage(str, Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    BOTH = "BOTH"

class ObjectType(str, Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"
    CTE = "CTE"
    UNKNOWN = "UNKNOWN"

class TableInfo(BaseModel):
    """Information about a table referenced in a query."""
    name: str  # Fully qualified table name
    usage: TableUsage
    object_type: ObjectType

class QueryTablesResult(BaseModel):
    """Result of table analysis for a single query."""
    metadata: QueryMetadata
    tables: List[TableInfo]
```

### 2. Table Extraction Logic

Add a new method to `LineageAnalyzer`:

```python
def analyze_tables(
    self,
    table_filter: Optional[str] = None,
) -> List[QueryTablesResult]
```

**Detection Logic:**

| Statement Type | Target Table Usage | Other Tables Usage | Target Object Type | Other Object Type |
|---------------|-------------------|-------------------|-------------------|-------------------|
| `SELECT` | N/A | INPUT | N/A | UNKNOWN |
| `INSERT INTO` | OUTPUT | INPUT | UNKNOWN | UNKNOWN |
| `CREATE TABLE` | OUTPUT | INPUT (from AS SELECT) | TABLE | UNKNOWN |
| `CREATE VIEW` | OUTPUT | INPUT | VIEW | UNKNOWN |
| `CREATE TABLE AS SELECT` (CTAS) | OUTPUT | INPUT | TABLE | UNKNOWN |
| `UPDATE` | OUTPUT | INPUT (from JOINs/WHERE) | UNKNOWN | UNKNOWN |
| `MERGE` | OUTPUT | INPUT | UNKNOWN | UNKNOWN |
| `DELETE` | OUTPUT | INPUT (from WHERE) | UNKNOWN | UNKNOWN |
| `DROP TABLE` | OUTPUT | N/A | TABLE | N/A |
| `DROP VIEW` | OUTPUT | N/A | VIEW | N/A |

**CTE Handling:**
- CTEs are extracted separately from the `WITH` clause
- Marked with `object_type="CTE"` and `usage="INPUT"`
- CTE names are NOT qualified (they're query-scoped)

### 3. Output Formats

#### Text Output (default)
```
Query 1/3: SELECT * FROM analytics.cust...

Tables:
  Table Name                  Usage    Type
  ────────────────────────────────────────────
  analytics.customers         INPUT    UNKNOWN
  analytics.orders            INPUT    UNKNOWN

Query 2/3: CREATE VIEW analytics.custo...

Tables:
  Table Name                  Usage    Type
  ────────────────────────────────────────────
  analytics.customer_summary  OUTPUT   VIEW
  analytics.customers         INPUT    UNKNOWN
  analytics.orders            INPUT    UNKNOWN
```

#### JSON Output
```json
{
  "queries": [
    {
      "query_index": 0,
      "query_preview": "SELECT * FROM analytics.cust...",
      "tables": [
        {"name": "analytics.customers", "usage": "INPUT", "object_type": "UNKNOWN"},
        {"name": "analytics.orders", "usage": "INPUT", "object_type": "UNKNOWN"}
      ]
    }
  ]
}
```

#### CSV Output
```csv
query_index,table_name,usage,object_type
0,analytics.customers,INPUT,UNKNOWN
0,analytics.orders,INPUT,UNKNOWN
1,analytics.customer_summary,OUTPUT,VIEW
1,analytics.customers,INPUT,UNKNOWN
```

### 4. CLI Command Structure

```bash
# Basic usage
uv run sqlglider tables query.sql

# With dialect
uv run sqlglider tables query.sql --dialect postgres

# JSON output
uv run sqlglider tables query.sql --output-format json

# Export to file
uv run sqlglider tables query.sql --output-format csv --output-file tables.csv

# Filter by table
uv run sqlglider tables query.sql --table customers

# With templating
uv run sqlglider tables query.sql --templater jinja --var schema=analytics
```

## Implementation Steps

- [x] **Step 1: Add Data Models**
  - Add `TableUsage` enum to `analyzer.py`
  - Add `ObjectType` enum to `analyzer.py`
  - Add `TableInfo` model to `analyzer.py`
  - Add `QueryTablesResult` model to `analyzer.py`

- [x] **Step 2: Implement Table Analysis Method**
  - Add `analyze_tables()` method to `LineageAnalyzer` class
  - Implement statement type detection (SELECT, INSERT, CREATE, UPDATE, etc.)
  - Implement table usage classification (INPUT, OUTPUT, BOTH)
  - Implement object type detection (TABLE, VIEW, UNKNOWN)
  - Handle multi-query files with `_iterate_queries()`

- [x] **Step 3: Create Table Formatters**
  - Create `TableTextFormatter` class in `formatters.py`
  - Create `TableJsonFormatter` class in `formatters.py`
  - Create `TableCsvFormatter` class in `formatters.py`

- [x] **Step 4: Add CLI Command**
  - Add `tables` command to `cli.py`
  - Include all standard options (dialect, output-format, output-file, templater, var, vars-file)
  - Add `--table` filter option
  - Follow existing error handling patterns

- [x] **Step 5: Update Configuration**
  - Verify config system works with new command (should work out of box)
  - Update `sqlglider.toml.example` if needed

- [x] **Step 6: Add Tests**
  - Add unit tests for `analyze_tables()` method in `tests/sqlglider/lineage/test_analyzer.py`
  - Add tests for new formatters in `tests/sqlglider/lineage/test_formatters.py`
  - Add CLI integration tests in `tests/sqlglider/test_cli.py`
  - Test SQL fixtures: SELECT, INSERT, CREATE TABLE, CREATE VIEW, UPDATE, MERGE, multi-query

- [x] **Step 7: Update Documentation**
  - Update `ARCHITECTURE.md` with new command
  - Update `CLAUDE.md` with new command usage
  - Update `README.md` with new command

## Files to Modify

| File | Changes |
|------|---------|
| `src/sqlglider/lineage/analyzer.py` | Add models and `analyze_tables()` method |
| `src/sqlglider/lineage/formatters.py` | Add `TableTextFormatter`, `TableJsonFormatter`, `TableCsvFormatter` |
| `src/sqlglider/cli.py` | Add `tables` command |
| `tests/sqlglider/lineage/test_analyzer.py` | Add tests for `analyze_tables()` |
| `tests/sqlglider/lineage/test_formatters.py` | Add tests for table formatters |
| `tests/sqlglider/test_cli.py` | Add CLI integration tests |
| `ARCHITECTURE.md` | Document new command |
| `CLAUDE.md` | Add CLI usage examples |
| `README.md` | Add user documentation |

## Testing Strategy

### Unit Tests
- Test `analyze_tables()` with various SQL statement types
- Test table usage classification (INPUT, OUTPUT, BOTH scenarios)
- Test object type detection (TABLE, VIEW)
- Test multi-query file handling
- Test table name qualification
- Test case-insensitive table filtering

### Integration Tests
- Test CLI command with various options
- Test output formats (text, json, csv)
- Test templating integration
- Test config file integration
- Test error handling (invalid file, parse errors)

## Resolved Questions

1. **UNKNOWN object type**: Use `"UNKNOWN"` when we can't determine the type ✓
2. **CTEs**: Include with `object_type="CTE"` ✓
3. **Subqueries**: Include all tables recursively ✓
4. **Table aliases**: Show only original fully-qualified name ✓
5. **Multi-query files**: Show tables per query ✓
