# Plan: File-Scoped Schema Context for SQL Lineage Analyzer

**Status:** Completed

## Summary

Add file-scoped schema context to the SQL Glider lineage analyzer so that SQLGlot can correctly expand `SELECT *` and trace cross-statement references when a file contains multiple related statements.

## Problem

When analyzing this SQL:
```sql
CREATE TEMPORARY VIEW first_view AS (SELECT a, b, c FROM source_table);
CREATE TEMPORARY VIEW second_view AS
WITH first_view_cte AS (
    SELECT *, row_number() OVER (PARTITION BY a ORDER BY b DESC) AS row_num
    FROM first_view
)
SELECT * FROM first_view_cte WHERE c = 1;
```

**Previous output:** `* -> second_view.*` (useless - no column-level lineage)
**Expected output:** `first_view.a -> second_view.a`, `first_view.b -> second_view.b`, etc.

## Root Cause

SQLGlot's `lineage()` function accepts a `schema` parameter that provides table/view column definitions. Without this schema context, SQLGlot cannot expand `SELECT *` to actual column names.

## Solution

Build up schema context incrementally as CREATE VIEW/TABLE statements are processed, then pass that schema to subsequent `lineage()` calls.

---

## Implementation Steps

### 1. Add Schema Instance Variable

- [x] Add `_file_schema: Dict[str, Dict[str, str]] = {}` to `LineageAnalyzer.__init__()`

### 2. Add Schema Extraction Methods

- [x] `_extract_schema_from_statement()` - Extract columns from CREATE VIEW/TABLE AS SELECT
- [x] `_extract_columns_from_select()` - Extract column names from SELECT projections
- [x] `_resolve_star_columns()` - Resolve SELECT * from file schema or CTEs
- [x] `_resolve_source_columns()` - Resolve columns from a single source (table, subquery)
- [x] `_resolve_qualified_star()` - Resolve table-qualified star (e.g., `t.*`)
- [x] `_extract_subquery_columns()` - Extract columns from subquery's SELECT
- [x] `_resolve_cte_columns()` - Resolve columns from CTE definitions
- [x] `_extract_cte_select_columns()` - Extract columns from CTE's SELECT

### 3. Integrate Schema Building into Analysis Loop

- [x] Reset `_file_schema = {}` at start of `analyze_queries()`
- [x] Call `_extract_schema_from_statement(expr)` in `finally` block AFTER analysis
- [x] Critical: Schema must be extracted AFTER analysis to avoid confusing SQLGlot

### 4. Pass Schema to lineage() Calls

- [x] Modify `_analyze_column_lineage_internal()` to pass schema:
```python
node = lineage(
    lineage_col,
    current_query_sql,
    dialect=self.dialect,
    schema=self._file_schema if self._file_schema else None,
)
```

### 5. Handle SELECT * in get_output_columns()

- [x] Handle `exp.Star` projections by resolving from file schema
- [x] Handle table-qualified stars (`t.*`) represented as `exp.Column` with `exp.Star` as `this`

---

## Edge Cases Handled

| Case | Implementation |
|------|----------------|
| `SELECT *` from unknown table | Returns empty columns, falls back to `*` behavior |
| Nested `SELECT *` through CTEs | Resolves CTE source from schema first |
| UNION in CREATE VIEW | Uses first branch's columns |
| Expressions without aliases | Uses SQL representation as column name |
| TEMPORARY VIEW | Treated same as regular VIEW |
| Multiple JOINs | Collects columns from all joined tables |
| LEFT/RIGHT/FULL OUTER JOIN | Same handling as INNER JOIN |
| CROSS JOIN | Same handling as INNER JOIN |
| Subquery in FROM clause | Extracts columns from inner SELECT |
| Table aliases (`v1 AS x`) | Resolves alias to actual table name |
| Schema-qualified names | Handles `schema.table` correctly |
| CTE referencing earlier CTE | Recursive CTE column resolution |
| `SELECT *, extra_col` | Combines * expansion with extra columns |
| Table-qualified `t.*` | Handles `v1.*` style syntax |
| LATERAL VIEW explode | Collects generated columns from `laterals` clause |
| LATERAL VIEW posexplode | Collects both position and element columns |
| Multiple LATERAL VIEWs | Collects columns from all LATERAL VIEWs |
| LATERAL VIEW OUTER | Same handling as regular LATERAL VIEW |
| LEFT SEMI JOIN | Only includes left table columns (right table excluded) |
| LEFT ANTI JOIN | Only includes left table columns (right table excluded) |

---

## Files Modified

| File | Changes |
|------|---------|
| `src/sqlglider/lineage/analyzer.py` | Added `_file_schema` instance variable; Added 9 schema extraction methods (including `_resolve_lateral_columns`); Modified `analyze_queries()` and `_analyze_column_lineage_internal()` and `get_output_columns()`; Added SEMI/ANTI join handling in `_resolve_star_columns()` |
| `tests/sqlglider/lineage/test_analyzer.py` | Added `TestFileSchemaExtraction` (9 tests), `TestCrossStatementLineage` (12 tests), `TestLateralViewColumnResolution` (5 tests), and `TestSemiAntiJoinColumnResolution` (3 tests) |

---

## Testing

### Test Classes Added

**TestFileSchemaExtraction (9 tests):**
- `test_extract_schema_from_create_view`
- `test_extract_schema_from_create_temporary_view`
- `test_extract_schema_from_create_table_as`
- `test_extract_schema_with_aliases`
- `test_extract_schema_select_star_from_known_table`
- `test_extract_schema_select_star_from_unknown_table`
- `test_schema_not_extracted_from_pure_select`
- `test_schema_not_extracted_from_insert`
- `test_schema_reset_between_analysis_calls`

**TestCrossStatementLineage (12 tests):**
- `test_view_referencing_earlier_view`
- `test_select_star_expansion_through_view`
- `test_cte_with_select_star_from_view`
- `test_window_function_with_select_star`
- `test_insert_from_view_lineage`
- `test_multi_hop_view_lineage`
- `test_original_problem_scenario`
- `test_select_star_from_join`
- `test_nested_ctes_and_views_with_select_star`
- `test_select_star_from_subquery`
- `test_table_qualified_star`
- `test_table_qualified_star_with_alias`

**TestLateralViewColumnResolution (5 tests):**
- `test_select_star_with_lateral_view_explode`
- `test_select_star_with_lateral_view_posexplode`
- `test_select_star_with_multiple_lateral_views`
- `test_select_star_with_lateral_view_outer`
- `test_lateral_view_with_join`

### Verification Commands

```bash
# Run all tests
uv run pytest --cov=sqlglider --cov-fail-under=80

# Run schema-related tests
uv run pytest tests/sqlglider/lineage/test_analyzer.py -k "schema or CrossStatement" -v

# Test the original problem scenario
uv run sqlglider graph build test_view_window_cte.sql --dialect spark --output graph.json
```

---

## Implementation Notes

### Critical Timing Issue

Initially, schema extraction was done BEFORE analysis in the loop, which caused SQLGlot to return unqualified column names (e.g., `customer_id` instead of `orders.customer_id`).

**Fix:** Move `_extract_schema_from_statement(expr)` to the `finally` block AFTER analysis completes. This ensures:
1. The current statement is analyzed without its own schema (correct behavior)
2. The schema is then extracted for use by subsequent statements

### Table-Qualified Star Handling

Table-qualified stars (`v1.*`) are represented differently than unqualified stars (`*`):
- `*` is `exp.Star`
- `v1.*` is `exp.Column` with `this` being `exp.Star` and `table` being `v1`

Both cases needed handling in:
- `_extract_columns_from_select()` for schema extraction
- `get_output_columns()` for lineage analysis output

### Subquery Column Resolution

For `SELECT * FROM (SELECT * FROM v1) sub`, the code:
1. Detects the subquery in `_resolve_source_columns()`
2. Extracts columns from the inner SELECT via `_extract_subquery_columns()`
3. Recursively resolves any `SELECT *` in the inner query

---

## Lessons Learned

1. **Timing matters:** Schema context must be built AFTER analyzing a statement, not before, to avoid confusing SQLGlot's lineage tracing.

2. **AST structure varies:** Different SQL constructs have different AST representations (e.g., `*` vs `t.*`), requiring multiple code paths.

3. **Recursive resolution:** CTEs and subqueries can reference other CTEs/views, requiring recursive column resolution.

4. **Edge cases compound:** JOINs + aliases + qualified stars can all combine, requiring careful handling of each case.
