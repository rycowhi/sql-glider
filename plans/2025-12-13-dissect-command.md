# Plan: SQL Dissect Command

**Status:** Completed
**Date:** 2025-12-13

## Overview

Add a `dissect` command to SQL Glider that decomposes SQL queries into constituent parts (CTEs, subqueries, main query, DML targets, source SELECTs, UNION branches, scalar subqueries) for unit testing and analysis.

## Use Cases

1. **Unit Testing**: Extract CTEs and subqueries to test individually
2. **DQL Extraction**: Get the SELECT from CTAS, CREATE VIEW, INSERT, MERGE statements
3. **Impact Analysis**: Understand query structure and component dependencies
4. **Refactoring**: Break apart complex queries for analysis

## Component Types to Extract

| Type | Description | Executable? | Location Context |
|------|-------------|-------------|------------------|
| `CTE` | Common Table Expression | Yes | WITH clause |
| `MAIN_QUERY` | Final SELECT statement | Yes | Top-level or after CTEs |
| `SUBQUERY` | Nested SELECT in FROM clause | Yes | FROM (subquery) AS alias |
| `SCALAR_SUBQUERY` | Single-value subquery | Yes | SELECT list, WHERE, HAVING |
| `TARGET_TABLE` | Output table for DML/DDL | No (name only) | INSERT INTO, CREATE TABLE/VIEW, MERGE INTO |
| `SOURCE_QUERY` | SELECT within DML/DDL | Yes | INSERT...SELECT, CTAS, CREATE VIEW |
| `UNION_BRANCH` | Individual SELECT in UNION | Yes | Part of UNION/UNION ALL |

## Indexing Strategy

- `query_index`: Which query in multi-query file (0-based)
- `component_index`: Sequential order within query (0, 1, 2...)
- `parent_index`: Index of parent component (for nested subqueries)
- `depth`: Nesting level (0 = top-level, 1+ = nested)

Order: CTEs (by declaration) → TARGET_TABLE → SOURCE_QUERY → MAIN_QUERY → UNION_BRANCHES → SUBQUERIES (depth-first)

## Sample Output Formats

### Example SQL for Demonstration

```sql
WITH order_totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
),
customer_segments AS (
    SELECT
        ot.customer_id,
        ot.total,
        (SELECT name FROM customers c WHERE c.id = ot.customer_id) AS customer_name
    FROM order_totals ot
    WHERE ot.total > 100
)
INSERT INTO analytics.premium_customers
SELECT customer_id, customer_name, total
FROM customer_segments
WHERE total > (SELECT AVG(total) FROM order_totals)
```

---

### JSON Output (`--output-format json`)

```json
{
  "queries": [
    {
      "query_index": 0,
      "query_preview": "WITH order_totals AS ( SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY...",
      "statement_type": "INSERT",
      "total_components": 6,
      "components": [
        {
          "component_type": "CTE",
          "component_index": 0,
          "name": "order_totals",
          "sql": "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
          "parent_index": null,
          "depth": 0,
          "is_executable": true,
          "dependencies": [],
          "location": "WITH clause"
        },
        {
          "component_type": "CTE",
          "component_index": 1,
          "name": "customer_segments",
          "sql": "SELECT ot.customer_id, ot.total, (SELECT name FROM customers AS c WHERE c.id = ot.customer_id) AS customer_name FROM order_totals AS ot WHERE ot.total > 100",
          "parent_index": null,
          "depth": 0,
          "is_executable": true,
          "dependencies": ["order_totals"],
          "location": "WITH clause"
        },
        {
          "component_type": "SCALAR_SUBQUERY",
          "component_index": 2,
          "name": "customer_name",
          "sql": "SELECT name FROM customers AS c WHERE c.id = ot.customer_id",
          "parent_index": 1,
          "depth": 1,
          "is_executable": true,
          "dependencies": [],
          "location": "SELECT list in CTE 'customer_segments'"
        },
        {
          "component_type": "TARGET_TABLE",
          "component_index": 3,
          "name": "analytics.premium_customers",
          "sql": "analytics.premium_customers",
          "parent_index": null,
          "depth": 0,
          "is_executable": false,
          "dependencies": [],
          "location": "INSERT INTO target"
        },
        {
          "component_type": "SOURCE_QUERY",
          "component_index": 4,
          "name": null,
          "sql": "SELECT customer_id, customer_name, total FROM customer_segments WHERE total > (SELECT AVG(total) FROM order_totals)",
          "parent_index": null,
          "depth": 0,
          "is_executable": true,
          "dependencies": ["customer_segments", "order_totals"],
          "location": "INSERT source SELECT"
        },
        {
          "component_type": "SCALAR_SUBQUERY",
          "component_index": 5,
          "name": null,
          "sql": "SELECT AVG(total) FROM order_totals",
          "parent_index": 4,
          "depth": 1,
          "is_executable": true,
          "dependencies": ["order_totals"],
          "location": "WHERE clause in SOURCE_QUERY"
        }
      ],
      "original_sql": "WITH order_totals AS (...full SQL...)"
    }
  ]
}
```

---

### Text Output (`--output-format text`, default)

```
Query 0 (INSERT): WITH order_totals AS ( SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY...
┏━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Index ┃ Type             ┃ Name                       ┃ Depth ┃ Exec?  ┃ Location                            ┃ SQL Preview                               ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 0     │ CTE              │ order_totals               │ 0     │ Yes    │ WITH clause                         │ SELECT customer_id, SUM(amount) AS tot... │
│ 1     │ CTE              │ customer_segments          │ 0     │ Yes    │ WITH clause                         │ SELECT ot.customer_id, ot.total, (SEL... │
│ 2     │ SCALAR_SUBQUERY  │ customer_name              │ 1     │ Yes    │ SELECT list in CTE 'customer_seg... │ SELECT name FROM customers AS c WHERE... │
│ 3     │ TARGET_TABLE     │ analytics.premium_customers│ 0     │ No     │ INSERT INTO target                  │ analytics.premium_customers               │
│ 4     │ SOURCE_QUERY     │ -                          │ 0     │ Yes    │ INSERT source SELECT                │ SELECT customer_id, customer_name, to... │
│ 5     │ SCALAR_SUBQUERY  │ -                          │ 1     │ Yes    │ WHERE clause in SOURCE_QUERY        │ SELECT AVG(total) FROM order_totals       │
└───────┴──────────────────┴────────────────────────────┴───────┴────────┴─────────────────────────────────────┴───────────────────────────────────────────┘
Total components: 6
```

---

### CSV Output (`--output-format csv`)

```csv
query_index,component_index,component_type,name,depth,is_executable,location,dependencies,sql
0,0,CTE,order_totals,0,true,WITH clause,,"SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
0,1,CTE,customer_segments,0,true,WITH clause,order_totals,"SELECT ot.customer_id, ot.total, (SELECT name FROM customers AS c WHERE c.id = ot.customer_id) AS customer_name FROM order_totals AS ot WHERE ot.total > 100"
0,2,SCALAR_SUBQUERY,customer_name,1,true,"SELECT list in CTE 'customer_segments'",,"SELECT name FROM customers AS c WHERE c.id = ot.customer_id"
0,3,TARGET_TABLE,analytics.premium_customers,0,false,INSERT INTO target,,analytics.premium_customers
0,4,SOURCE_QUERY,,0,true,INSERT source SELECT,customer_segments;order_totals,"SELECT customer_id, customer_name, total FROM customer_segments WHERE total > (SELECT AVG(total) FROM order_totals)"
0,5,SCALAR_SUBQUERY,,1,true,WHERE clause in SOURCE_QUERY,order_totals,"SELECT AVG(total) FROM order_totals"
```

---

### UNION Example

**Input SQL:**
```sql
SELECT id, name FROM customers WHERE status = 'active'
UNION ALL
SELECT id, name FROM archived_customers WHERE archived_date > '2024-01-01'
UNION
SELECT id, name FROM pending_customers
```

**JSON Output:**
```json
{
  "queries": [
    {
      "query_index": 0,
      "query_preview": "SELECT id, name FROM customers WHERE status = 'active' UNION ALL SELECT id, name...",
      "statement_type": "SELECT",
      "total_components": 4,
      "components": [
        {
          "component_type": "MAIN_QUERY",
          "component_index": 0,
          "name": null,
          "sql": "SELECT id, name FROM customers WHERE status = 'active' UNION ALL SELECT id, name FROM archived_customers WHERE archived_date > '2024-01-01' UNION SELECT id, name FROM pending_customers",
          "parent_index": null,
          "depth": 0,
          "is_executable": true,
          "dependencies": [],
          "location": "Top-level query"
        },
        {
          "component_type": "UNION_BRANCH",
          "component_index": 1,
          "name": "branch_0",
          "sql": "SELECT id, name FROM customers WHERE status = 'active'",
          "parent_index": 0,
          "depth": 1,
          "is_executable": true,
          "dependencies": [],
          "location": "UNION branch 1 of 3"
        },
        {
          "component_type": "UNION_BRANCH",
          "component_index": 2,
          "name": "branch_1",
          "sql": "SELECT id, name FROM archived_customers WHERE archived_date > '2024-01-01'",
          "parent_index": 0,
          "depth": 1,
          "is_executable": true,
          "dependencies": [],
          "location": "UNION branch 2 of 3 (UNION ALL)"
        },
        {
          "component_type": "UNION_BRANCH",
          "component_index": 3,
          "name": "branch_2",
          "sql": "SELECT id, name FROM pending_customers",
          "parent_index": 0,
          "depth": 1,
          "is_executable": true,
          "dependencies": [],
          "location": "UNION branch 3 of 3 (UNION)"
        }
      ],
      "original_sql": "SELECT id, name FROM customers WHERE status = 'active' UNION ALL..."
    }
  ]
}
```

## Files to Create

```
src/sqlglider/dissection/
├── __init__.py           # Public API exports
├── models.py             # Pydantic models (ComponentType, SQLComponent, QueryDissectionResult)
├── analyzer.py           # DissectionAnalyzer class
└── formatters.py         # Text/JSON/CSV formatters

tests/sqlglider/dissection/
├── __init__.py
├── test_models.py
├── test_analyzer.py
└── test_formatters.py
```

## Files to Modify

- [cli.py](src/sqlglider/cli.py) - Add `dissect` command
- [CLAUDE.md](CLAUDE.md) - Document new command
- [ARCHITECTURE.md](ARCHITECTURE.md) - Document dissection module

## Data Models

### ComponentType (Enum)
```python
class ComponentType(str, Enum):
    CTE = "CTE"
    MAIN_QUERY = "MAIN_QUERY"
    SUBQUERY = "SUBQUERY"
    SCALAR_SUBQUERY = "SCALAR_SUBQUERY"
    TARGET_TABLE = "TARGET_TABLE"
    SOURCE_QUERY = "SOURCE_QUERY"
    UNION_BRANCH = "UNION_BRANCH"
```

### SQLComponent (Pydantic Model)
```python
class SQLComponent(BaseModel):
    component_type: ComponentType
    component_index: int
    name: Optional[str]           # CTE name, alias, target table
    sql: str                      # Extracted SQL
    parent_index: Optional[int]   # For nested components
    depth: int = 0                # Nesting depth
    is_executable: bool = True    # Can run standalone?
    dependencies: List[str] = []  # CTE names this depends on
    location: str                 # Human-readable location context
```

### QueryDissectionResult (Pydantic Model)
```python
class QueryDissectionResult(BaseModel):
    metadata: QueryMetadata
    components: List[SQLComponent]
    original_sql: str
```

## CLI Command

### dissect
```bash
# Basic usage
sqlglider dissect query.sql

# JSON output
sqlglider dissect query.sql -f json

# CSV output
sqlglider dissect query.sql -f csv

# From stdin
echo "SELECT * FROM t" | sqlglider dissect

# Export to file
sqlglider dissect query.sql -f json -o dissected.json

# With templating
sqlglider dissect query.sql --templater jinja --var schema=prod

# Different dialect
sqlglider dissect query.sql --dialect postgres
```

## Implementation Steps

### Phase 1: Core Models
- [ ] Create `src/sqlglider/dissection/` directory
- [ ] Create `__init__.py` with exports
- [ ] Implement `models.py` with ComponentType, SQLComponent, QueryMetadata, QueryDissectionResult

### Phase 2: Analyzer
- [ ] Create `analyzer.py` with DissectionAnalyzer class
- [ ] Implement CTE extraction
- [ ] Implement target table extraction (INSERT/CTAS/MERGE)
- [ ] Implement source query extraction
- [ ] Implement main query extraction
- [ ] Implement UNION branch extraction
- [ ] Implement subquery extraction (FROM clause, recursive depth-first)
- [ ] Implement scalar subquery extraction (SELECT list, WHERE, HAVING)
- [ ] Implement dependency tracking
- [ ] Implement location context generation

### Phase 3: Formatters
- [ ] Create `formatters.py`
- [ ] Implement DissectionTextFormatter (Rich tables)
- [ ] Implement DissectionJsonFormatter
- [ ] Implement DissectionCsvFormatter

### Phase 4: CLI Integration
- [ ] Add `dissect` command to cli.py
- [ ] Support all standard options (dialect, output-format, output-file, templater, var, vars-file)
- [ ] Support stdin input

### Phase 5: Testing
- [ ] Create test directory structure
- [ ] Write tests for models (serialization, validation)
- [ ] Write tests for analyzer (CTEs, subqueries, scalar subqueries, DML, DDL, UNIONs)
- [ ] Write tests for formatters (text, JSON, CSV)
- [ ] Write CLI integration tests
- [ ] Ensure 80%+ coverage

### Phase 6: Documentation
- [ ] Update CLAUDE.md with dissect examples
- [ ] Update ARCHITECTURE.md with dissection module design
- [ ] Update README.md with dissect command documentation

## Key Design Decisions

1. **Sequential indexing**: Use single `component_index` across all types (simpler than per-type indices)
2. **Dependencies**: Track only CTE dependencies (not all table refs - that's what `lineage` command is for)
3. **original_sql**: Store in result for reference (no reassemble command needed with this)
4. **UNION branches**: Extract as separate components for testability
5. **Scalar subqueries**: Extract separately from FROM-clause subqueries for clarity
6. **Location field**: Human-readable context explaining where each component lives in the query

## Test Scenarios

1. Simple SELECT with CTEs
2. Nested CTEs (CTE referencing another CTE)
3. INSERT INTO ... SELECT
4. CREATE TABLE AS SELECT
5. CREATE VIEW AS SELECT
6. MERGE with complex USING clause
7. UNION/UNION ALL (2+ branches)
8. Nested subqueries (3+ levels deep)
9. Scalar subqueries in SELECT list
10. Scalar subqueries in WHERE clause
11. Multi-query files
12. Empty/comment-only SQL (error handling)

## References

- [analyzer.py](src/sqlglider/lineage/analyzer.py) - SQLGlot parsing patterns, CTE extraction
- [formatters.py](src/sqlglider/lineage/formatters.py) - Formatter patterns to follow
- [models.py](src/sqlglider/graph/models.py) - Pydantic model patterns
- [cli.py](src/sqlglider/cli.py) - CLI command structure
