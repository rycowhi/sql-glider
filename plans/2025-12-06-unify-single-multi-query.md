# Plan: Unify Single-Query and Multi-Query Lineage Processing

**Status:** Completed
**Date:** 2025-12-06

## Executive Summary

**Problem:** SQL Glider has ~200 lines of duplicated code across 6 analyzer methods, 3 formatter classes, and 12 CLI code paths for handling single vs multi-query files.

**Solution:** Treat all files as multi-query (even single-statement files), use unified data models, and consolidate to 1 analyzer method + 1 formatter method per class.

**Key Changes:**
- ✅ New unified data models: `QueryLineageResult`, `LineageItem`, `QueryMetadata`
- ✅ Single analyzer method: `analyze_queries()` (replaces 6 methods)
- ✅ Single formatter method: `format()` per class (replaces 4-6 methods each)
- ✅ CLI: 3 code paths instead of 12
- ✅ CSV format: `query_index,output_column,source_column` (one row per source)
- ✅ Additional refactoring: Query Iterator Pattern + Consolidated Models
- ✅ Breaking changes: User confirmed backward compatibility not a concern

**Impact:** ~50% reduction in code complexity, easier maintenance, cleaner API.

## Overview

Currently, SQL Glider maintains separate code paths for single-query and multi-query SQL files, resulting in significant code duplication across analyzer methods, formatters, and CLI logic. This refactoring will unify the processing logic by treating all files as multi-query (even single-statement files), adding `query_index` to all outputs, and eliminating duplicate code.

## Goals

1. **Add `query_index` to all output formats** (text, JSON, CSV) - even for single-query files
2. **Unify output format structure** - single and multi-query use identical data models and formatting
3. **Use a single Python function** for lineage derivation instead of 6 separate methods
4. **Eliminate code duplication** across analyzer, formatters, and CLI

## Analysis Summary

### Current Duplication

**Analyzer (`analyzer.py`):**
- 3 pairs of nearly identical methods (6 total):
  - `analyze_column_lineage()` vs `analyze_all_queries()`
  - `analyze_reverse_lineage()` vs `analyze_all_queries_reverse()`
  - `analyze_table_lineage()` vs `analyze_all_queries_table_lineage()`
- Each `analyze_all_queries_*()` method has ~70 lines of identical boilerplate:
  - Query iteration loop
  - `self.expr` swapping logic
  - Table filtering (Lines 200-212, 270-278, 350-358)
  - Query preview generation (Lines 231-234, 312-315, 365-368)
  - try/finally blocks

**Formatters (`formatters.py`):**
- Each formatter class has 4-6 methods with duplicated logic:
  - `format()` vs `format_multi_query()`
  - `format_table()` vs `format_multi_query_table()`
- Multi-query methods simply wrap single-query logic with query metadata headers

**CLI (`cli.py`):**
- 12 distinct code paths based on:
  - Single vs multi-query
  - Column vs table level
  - Forward vs reverse lineage
- Lines 151-225: Complex nested if/else branching

### Key Insight

**All code paths use the same core SQLGlot lineage derivation.** Multi-query methods just wrap single-query analysis with iteration and metadata. This is unnecessary complexity.

## Design Decisions

### 1. Unified Data Model

**Decision:** Replace all 4 existing models with 3 new unified models.

**New Models:**
```python
class LineageItem(BaseModel):
    """Represents a single lineage relationship (output -> source)."""
    output_name: str
    source_name: str

class QueryMetadata(BaseModel):
    """Query execution context."""
    query_index: int
    query_preview: str

class QueryLineageResult(BaseModel):
    """Complete lineage result for a single query."""
    metadata: QueryMetadata
    lineage_items: List[LineageItem]
    level: Literal["column", "table"]
```

**Rationale:**
- Single-query files will have `query_index=0` and one item in results list
- Works for both column and table lineage (just different output_name/source_name values)
- Flattens output -> [sources] into [(output, source1), (output, source2)] for easier CSV generation
- Simplifies formatters to handle only one data structure

**Impact:**
- DELETE old models: `ColumnLineage`, `TableLineage`, `QueryLineage`, `QueryTableLineage`
- All analyzer methods return `List[QueryLineageResult]`
- Formatters only need one method per format type

### 2. Single Unified Analysis Method

**Decision:** Create a single `analyze_queries()` method that replaces all 6 existing methods.

**Method Signature:**
```python
def analyze_queries(
    self,
    level: Literal["column", "table"] = "column",
    column: Optional[str] = None,
    source_column: Optional[str] = None,
    table_filter: Optional[str] = None,
) -> List[QueryLineageResult]:
    """
    Unified lineage analysis for single or multi-query files.

    Args:
        level: Analysis level ("column" or "table")
        column: Target output column for forward lineage
        source_column: Source column for reverse lineage (impact analysis)
        table_filter: Filter queries to those referencing this table

    Returns:
        List of QueryLineageResult objects (one per query)
    """
```

**Rationale:**
- Single entry point for all lineage analysis
- Handles single/multi-query transparently
- Consolidates all filtering and iteration logic
- Easier to test and maintain

**Implementation Strategy:**
- Extract common loop logic into helper methods
- Use query iterator pattern for filtering and preview generation
- Use internal dispatching based on `level` and presence of `source_column`

### 3. Unified Formatters

**Decision:** Each formatter class has only 1 method:
- `format(results: List[QueryLineageResult]) -> str`

**Rationale:**
- Single-query files produce a list with one item (`query_index=0`)
- Formatter iterates over the list regardless of length
- Level distinction handled by checking `results[0].level`

**Impact:**
- DELETE methods: `format_multi_query()`, `format_multi_query_table()`, `format_table()`
- Update formatters to always display query metadata
- For single-query files, output shows "Query 0" header

### 4. Simplified CLI Logic

**Decision:** Collapse 12 code paths into 3:
1. Text formatting
2. JSON formatting
3. CSV formatting

**Rationale:**
- Remove `has_multiple_queries` branching entirely
- Always call `analyze_queries()` with appropriate parameters
- Formatters handle single/multi and column/table transparently

## Implementation Plan

### Phase 1: Analyzer Refactoring

**File:** `src/sqlglider/lineage/analyzer.py`

- [ ] **Create new consolidated data models (replace existing 4 models):**
  ```python
  class LineageItem(BaseModel):
      """Represents a single lineage relationship (output -> source)."""
      output_name: str = Field(..., description="Output column/table name")
      source_name: str = Field(..., description="Source column/table name")

  class QueryMetadata(BaseModel):
      """Query execution context."""
      query_index: int = Field(..., description="0-based query index")
      query_preview: str = Field(..., description="First 100 chars of query")

  class QueryLineageResult(BaseModel):
      """Complete lineage result for a single query."""
      metadata: QueryMetadata
      lineage_items: List[LineageItem]
      level: Literal["column", "table"]
  ```
  - DELETE old models: `ColumnLineage`, `TableLineage`, `QueryLineage`, `QueryTableLineage`
  - This unified structure works for both column and table lineage

- [ ] **Extract helper methods:**
  - `_generate_query_preview(expr: Expression) -> str` - Extract Lines 231-234
  - `_filter_by_table(expr: Expression, table_filter: str) -> bool` - Extract Lines 200-212
  - `_should_include_query(expr, column, source_column, table_filter) -> bool` - Combine filtering logic

- [ ] **Create Query Iterator Pattern (Additional Refactoring #1):**
  ```python
  def _iterate_queries(
      self,
      table_filter: Optional[str] = None,
  ) -> Iterator[Tuple[int, Expression, str]]:
      """
      Iterate over queries with filtering and preview generation.

      Yields: (query_index, expression, query_preview)
      """
      for idx, expr in enumerate(self.expressions):
          # Apply table filter
          if table_filter and not self._filter_by_table(expr, table_filter):
              continue

          # Generate preview
          preview = self._generate_query_preview(expr)

          yield idx, expr, preview
  ```

- [ ] **Create unified `analyze_queries()` method:**
  - Method signature:
    ```python
    def analyze_queries(
        self,
        level: Literal["column", "table"] = "column",
        column: Optional[str] = None,
        source_column: Optional[str] = None,
        table_filter: Optional[str] = None,
    ) -> List[QueryLineageResult]:
    ```
  - Implementation:
    - Use `_iterate_queries()` for query iteration
    - For each query, temporarily swap `self.expr`
    - Based on `level` and `source_column`, dispatch to internal analysis methods
    - Convert results to `QueryLineageResult` with `LineageItem` list
  - Replaces all 6 existing methods

- [ ] **Remove old methods completely:**
  - DELETE: `analyze_column_lineage()`, `analyze_reverse_lineage()`, `analyze_table_lineage()`, `analyze_all_queries()`, `analyze_all_queries_reverse()`, `analyze_all_queries_table_lineage()`
  - No deprecation needed per user decision

- [ ] **Update internal analysis methods to return flat list:**
  - Modify internal methods to return `List[LineageItem]` instead of `List[ColumnLineage]`
  - This flattens output_column -> [source1, source2] into [(output, source1), (output, source2)]

### Phase 2: Formatter Refactoring

**File:** `src/sqlglider/lineage/formatters.py`

- [ ] **Refactor to single `format()` method per formatter:**
  - Delete all `format_multi_query()`, `format_multi_query_table()`, `format_table()` methods
  - Each formatter class has ONE method: `format(results: List[QueryLineageResult]) -> str`
  - Level distinction (column vs table) handled by checking `results[0].level` if needed

- [ ] **Refactor `TextFormatter.format()`:**
  - Input: `List[QueryLineageResult]`
  - For each query result:
    - Display query header: `==========\nQuery {idx}: {preview}\n==========`
    - For column level: Display `output_name` -> `source_name` relationships
    - For table level: Display `output_table` -> `source_tables` relationships
  - Single-query files (len=1) still show "Query 0" header (user confirmed)

- [ ] **Refactor `JsonFormatter.format()`:**
  - Input: `List[QueryLineageResult]`
  - Output structure:
    ```json
    {
      "queries": [
        {
          "query_index": 0,
          "query_preview": "SELECT ...",
          "level": "column",
          "lineage": [
            {"output_name": "table.col_a", "source_name": "src.col_x"},
            {"output_name": "table.col_a", "source_name": "src.col_y"}
          ]
        }
      ]
    }
    ```
  - Works for both column and table level (just different `output_name`/`source_name` values)

- [ ] **Refactor `CsvFormatter.format()`:**
  - Input: `List[QueryLineageResult]`
  - **Column-level output:**
    ```csv
    query_index,output_column,source_column
    0,table.column_a,source_table.column_x
    0,table.column_a,source_table.column_y
    0,table.column_b,source_table2.column_z
    ```
  - **Table-level output:**
    ```csv
    query_index,output_table,source_table
    0,query_result,customers
    0,query_result,orders
    ```
  - Implementation:
    - Check `results[0].level` to determine column headers
    - Iterate through all queries and lineage items
    - Each `LineageItem` becomes one row
  - **Important:** Each source gets its own row (already flattened in `LineageItem` model)

### Phase 3: CLI Simplification

**File:** `src/sqlglider/cli.py`

- [ ] **Remove `has_multiple_queries` branching:**
  - Delete Line 152: `has_multiple_queries = len(analyzer.expressions) > 1`
  - Remove ALL conditional logic based on this variable (Lines 155-224)

- [ ] **Replace entire analysis section (Lines 155-224) with simplified logic:**
  ```python
  # Single unified call for all cases
  results = analyzer.analyze_queries(
      level=level,
      column=column,
      source_column=source_column,
      table_filter=table_filter
  )

  # Single formatter call based on output format
  if output_format == "text":
      formatted = TextFormatter.format(results)
  elif output_format == "json":
      formatted = JsonFormatter.format(results)
  else:  # csv
      formatted = CsvFormatter.format(results)
  ```

- [ ] **Result:** Reduce from 12 code paths to 3 (based on output_format only)

- [ ] **Update formatter instantiation:**
  - All formatters now use the same `format()` method signature
  - No need to check `level` - formatters handle this internally

### Phase 4: Testing

**Files:** `tests/sqlglider/lineage/test_analyzer.py`, `tests/sqlglider/lineage/test_formatters.py`, `tests/sqlglider/test_cli.py`

- [ ] **Update existing analyzer tests:**
  - Replace imports: `QueryLineage` → `QueryLineageResult`, `ColumnLineage` → `LineageItem`
  - Update all test assertions to expect `QueryLineageResult` with `lineage_items` list
  - Change from `results[0].output_column` to `results[0].lineage_items[0].output_name`
  - Change from `results[0].source_columns` to flattened `LineageItem` list
  - Verify `query_index=0` for single-query tests
  - Verify `query_index` sequencing for multi-query tests

- [ ] **Update test method calls:**
  - Replace all calls to old methods with `analyze_queries()`
  - Examples:
    - `analyze_column_lineage(column="foo")` → `analyze_queries(level="column", column="foo")`
    - `analyze_reverse_lineage(source_column="bar")` → `analyze_queries(level="column", source_column="bar")`
    - `analyze_table_lineage()` → `analyze_queries(level="table")`
    - `analyze_all_queries()` → `analyze_queries()`

- [ ] **Update formatter tests:**
  - Update test data to use `QueryLineageResult` model
  - Test CSV output format matches new structure (one row per source)
  - Test JSON output structure matches new format
  - Test text output shows query headers for single-query files

- [ ] **Add new tests:**
  - Test `analyze_queries()` with all parameter combinations
  - Test single-query file produces `query_index=0`
  - Test formatters handle single-query (len=1) correctly
  - Test CSV row expansion (multiple sources → multiple rows)
  - Test query iterator pattern with table filtering

- [ ] **Test coverage verification:**
  - Run `uv run pytest --cov=sqlglider --cov-report=term-missing`
  - Ensure coverage remains >80%
  - Identify and fill any coverage gaps
  - Focus on new helper methods and query iterator

### Phase 5: Documentation

- [ ] **Update ARCHITECTURE.md:**
  - Document unified data model approach
  - Remove references to separate single/multi-query code paths
  - Update component diagrams if present

- [ ] **Update README.md:**
  - Update example outputs to show `query_index` field
  - Note that single-query files now show "Query 0" in text output
  - Add migration guide if breaking changes exist

- [ ] **Update CLAUDE.md:**
  - Update development guidelines for the new unified approach
  - Update testing patterns with new data models

- [ ] **Add docstrings:**
  - Document `analyze_queries()` method thoroughly
  - Add usage examples in docstrings
  - Document helper methods

## Additional Refactoring Opportunities

Beyond the user's original request, these improvements are APPROVED for implementation:

### 1. Extract Query Iterator Pattern ✅ APPROVED

**Problem:** Even after unification, we'll still have query iteration logic.

**Solution:** Create a query iterator method `_iterate_queries()` as shown in Phase 1.

**Benefits:**
- Centralizes filtering logic
- Easier to add new filter types (e.g., filter by CTE name)
- Cleaner separation of concerns

**Status:** IMPLEMENTING - This will be included in Phase 1

### 2. Consolidate Output Models ✅ APPROVED

**Problem:** We have 4 similar Pydantic models with duplicated patterns.

**Solution:** Replace with 3 new models: `LineageItem`, `QueryMetadata`, `QueryLineageResult`

**Benefits:**
- Single model for both column and table lineage
- Easier to extend with new metadata fields
- More consistent API

**Status:** IMPLEMENTING - This will be included in Phase 1

## Risks and Mitigation

### Risk 1: Breaking Changes to Output Format

**Impact:** Users parsing output may break if format changes significantly.

**Mitigation:**
- ✅ User confirmed backward compatibility is NOT a concern
- No legacy format support needed
- Clean break with updated documentation
- Consider semantic versioning bump (0.x.0 → 1.0.0)

### Risk 2: Performance Regression

**Impact:** Always processing as multi-query could add overhead for single-query files.

**Mitigation:**
- Profile before/after refactoring
- Optimize query iteration if needed (though overhead should be negligible)
- Add performance benchmarks to test suite

### Risk 3: Test Coverage Gaps

**Impact:** Refactoring complex code could introduce bugs if tests are insufficient.

**Mitigation:**
- Update tests incrementally with each phase
- Maintain >80% coverage throughout
- Add integration tests for end-to-end workflows

### Risk 4: Backward Compatibility

**Impact:** Existing code using analyzer as a library could break.

**Mitigation:**
- ✅ User confirmed backward compatibility is NOT a concern
- Old methods will be completely removed
- Clean break - users must update to new API
- Provide migration guide in documentation for API changes

## Success Criteria

- [ ] All single-query and multi-query files produce same output structure
- [ ] `query_index` field present in all outputs (text, JSON, CSV)
- [ ] Only one analyzer method: `analyze_queries()`
- [ ] Only one formatter method per format type: `format()`
- [ ] CLI has no `has_multiple_queries` branching
- [ ] Test coverage remains >80%
- [ ] All existing tests pass (with updates for new format)
- [ ] Documentation updated to reflect new approach

## Critical Files

### To Modify:
- [src/sqlglider/lineage/analyzer.py](src/sqlglider/lineage/analyzer.py) - Core refactoring
- [src/sqlglider/lineage/formatters.py](src/sqlglider/lineage/formatters.py) - Unify formatters
- [src/sqlglider/cli.py](src/sqlglider/cli.py) - Simplify CLI logic

### To Update:
- [tests/sqlglider/lineage/test_analyzer.py](tests/sqlglider/lineage/test_analyzer.py) - Update assertions
- [tests/sqlglider/lineage/test_formatters.py](tests/sqlglider/lineage/test_formatters.py) - Update assertions
- [tests/sqlglider/test_cli.py](tests/sqlglider/test_cli.py) - Update CLI tests
- [ARCHITECTURE.md](ARCHITECTURE.md) - Update architecture docs
- [README.md](README.md) - Update examples and usage
- [CLAUDE.md](CLAUDE.md) - Update development guidelines

## Implementation Notes

**Recommended Implementation Order:**
1. Start with analyzer refactoring (Phase 1) - core logic change
2. Update formatters (Phase 2) - depends on new analyzer output
3. Simplify CLI (Phase 3) - consumes refactored analyzer and formatters
4. Update tests throughout (Phase 4) - test each phase as you go
5. Update documentation last (Phase 5) - reflect final implementation

**Estimated Complexity:**
- **Phase 1 (Analyzer):** Medium - requires careful extraction of common logic
- **Phase 2 (Formatters):** Low - mostly removing code
- **Phase 3 (CLI):** Low - simplifying existing logic
- **Phase 4 (Tests):** Medium - many tests need updates
- **Phase 5 (Docs):** Low - straightforward updates

**Potential Challenges:**
- Handling edge cases in filtering logic during consolidation
- Updating all test fixtures to expect new format
- Ensuring CSV row expansion (one row per source) works correctly

## User Decisions (Confirmed)

1. **Output format breaking change:** ✅ YES - Single-query files will show `query_index=0` in all outputs. Backward compatibility is NOT a concern.

2. **CSV output format:** The desired CSV format is:

   **For column-level lineage:**
   ```csv
   query_index,output_column,source_column
   0,table.column_a,source_table.column_x
   0,table.column_a,source_table.column_y
   0,table.column_b,source_table2.column_z
   ```
   Where:
   - `output_column` is the fully qualified output column name
   - `source_column` is the fully qualified input/source column name
   - Each source column gets its own row (one row per source)
   - Note: This is different from current format which has separate `source_table` and `source_column` columns

   **For table-level lineage:**
   ```csv
   query_index,output_table,source_table
   0,query_result,customers
   0,query_result,orders
   ```
   Where:
   - `output_table` is the output table name
   - `source_table` is the source table name
   - Each source table gets its own row

3. **Backward compatibility:** ✅ REMOVE old methods completely. No deprecation needed.

4. **Additional refactoring:** ✅ YES - Implement all additional refactoring opportunities:
   - Query Iterator Pattern
   - Consolidate Output Models

5. **Legacy format flag:** ✅ NO - Clean break, no legacy format support needed.

## Conclusion

This refactoring will significantly improve SQL Glider's codebase by:
- ✅ Eliminating ~200 lines of duplicated code
- ✅ Reducing code complexity by ~50%
- ✅ Creating a simpler, more maintainable API
- ✅ Providing consistent behavior across all use cases
- ✅ Making future enhancements easier to implement

**Verdict:** The benefits far outweigh the costs. This refactoring aligns with DRY principles and will make the codebase much more maintainable.
