# Reverse Lineage (Impact Analysis) Implementation Plan

**Date:** 2024-12-05
**Status:** Completed
**Feature:** Reverse Lineage for Impact Analysis

## Overview

Add reverse lineage capability to SQL Glider, enabling impact analysis by showing which output columns are affected by a given source column. This feature complements the existing forward lineage (source tracking) with reverse lineage (impact tracking) while maintaining full backward compatibility with all existing printing and file output functionality.

## User Requirements

- Add reverse lineage feature mentioned in the column-level lineage plan
- Keep all compatibility for printing and file output
- Maintain existing CLI behavior and output formats (text, JSON, CSV)

## Design Approach

### Core Strategy: Graph Inversion with Semantic Field Reuse

The implementation leverages a clever design pattern:
1. **Run forward lineage on all columns** to build complete dependency graph
2. **Invert the graph** to create source → outputs mapping
3. **Reuse existing `ColumnLineage` model** with semantic swap:
   - Forward: `output_column` = query output, `source_columns` = table sources
   - Reverse: `output_column` = source column being analyzed, `source_columns` = affected outputs

This approach provides:
- **Maximum code reuse**: All formatters work without modification
- **Zero formatter changes**: Text, JSON, CSV all work as-is
- **Consistent API**: Same data structure for both directions
- **Simple mental model**: Just interpret fields differently

### CLI Interface Design

**Add `--source-column` parameter** (implies reverse mode):
```bash
# Forward lineage (existing)
sqlglider lineage query.sql --column customer_name

# Reverse lineage (new)
sqlglider lineage query.sql --source-column orders.customer_id

# Error case (mutually exclusive)
sqlglider lineage query.sql --column x --source-column y  # Validation error
```

**Rationale:**
- Intuitive: Parameter name clearly indicates reverse intent
- Self-documenting: `--source-column` naturally suggests "find where this source is used"
- Mutually exclusive with `--column`: Prevents confusion
- Backward compatible: Existing behavior unchanged

### Algorithm Design

**Run forward lineage, then invert:**
```python
def analyze_reverse_lineage(self, source_column: str) -> List[ColumnLineage]:
    # Step 1: Run forward lineage on all output columns
    forward_results = self.analyze_column_lineage(column=None)

    # Step 2: Build reverse mapping (source -> [affected outputs])
    reverse_map = {}
    for result in forward_results:
        for source in result.source_columns:
            if source not in reverse_map:
                reverse_map[source] = []
            reverse_map[source].append(result.output_column)

    # Step 3: Validate source exists
    if source_column not in reverse_map:
        available = sorted(reverse_map.keys())
        raise ValueError(
            f"Source column '{source_column}' not found in query sources. "
            f"Available source columns: {', '.join(available)}"
        )

    # Step 4: Return with semantic swap
    return [ColumnLineage(
        output_column=source_column,
        source_columns=sorted(reverse_map[source_column])
    )]
```

**Benefits:**
- Leverages existing robust forward lineage code
- Handles all SQLGlot edge cases automatically (CTEs, subqueries, complex expressions)
- Simple O(n) time complexity where n = number of output columns
- Easy to test (inherits verification from forward lineage)

## Output Examples

### Text Format
```
----------
orders.customer_id
----------
customer_id
segment
```
*Interpretation: "orders.customer_id affects output columns: customer_id, segment"*

### JSON Format
```json
{
  "columns": [
    {
      "output_column": "orders.customer_id",
      "source_columns": ["customer_id", "segment"]
    }
  ]
}
```

### CSV Format
```csv
output_column,source_table,source_column
orders.customer_id,,customer_id
orders.customer_id,,segment
```

**Note:** Headers remain the same for backward compatibility (no formatter changes), but semantics are reversed in reverse mode.

## Implementation Steps

### Phase 1: Core Implementation (Analyzer)

**File: `src/sqlglider/lineage/analyzer.py`** (~50 lines added)

- [ ] Add `analyze_reverse_lineage(source_column: str) -> List[ColumnLineage]` method
  - Call `analyze_column_lineage()` to get forward lineage for all columns
  - Build reverse mapping dictionary (source -> [affected outputs])
  - Validate source_column exists in reverse_map
  - Return `ColumnLineage` with semantic swap
  - Handle edge cases (no affected columns, multiple references)
- [ ] Add comprehensive docstring explaining semantic swap
- [ ] Add error handling with helpful messages listing available sources

### Phase 2: CLI Integration

**File: `src/sqlglider/cli.py`** (~30 lines modified)

- [ ] Add `--source-column` parameter:
  ```python
  source_column: Optional[str] = typer.Option(
      None,
      "--source-column",
      "-s",
      help="Source column for reverse lineage (impact analysis)",
  )
  ```
- [ ] Add mutual exclusivity validation:
  ```python
  if column and source_column:
      console.print(
          "[red]Error:[/red] Cannot specify both --column and --source-column. "
          "Use --column for forward lineage or --source-column for reverse lineage.",
          stderr=True,
      )
      raise typer.Exit(1)
  ```
- [ ] Add reverse lineage branch in column-level section:
  ```python
  if level == "column":
      if source_column:
          # Reverse lineage (impact analysis)
          results = analyzer.analyze_reverse_lineage(source_column)
      else:
          # Forward lineage (existing)
          results = analyzer.analyze_column_lineage(column)
      # Format output (unchanged - formatters are direction-agnostic)
  ```
- [ ] Update docstring with reverse lineage examples
- [ ] Update help text for `--column` to clarify it's for forward lineage

### Phase 3: Documentation Updates

**File: `ARCHITECTURE.md`** (~100 lines added)

- [ ] Add "Reverse Lineage Algorithm" section under Core Components
- [ ] Add design decision for reverse lineage via graph inversion
- [ ] Update CLI usage section with reverse lineage examples
- [ ] Remove "Reverse lineage" from Future Enhancements (now implemented)

**File: `CLAUDE.md`** (~20 lines modified)

- [ ] Update CLI Usage section with reverse lineage examples
- [ ] Add reverse lineage to development guidelines

**File: `README.md`** (~50 lines added)

- [ ] Add reverse lineage feature section
- [ ] Add usage examples for impact analysis
- [ ] Add use cases (change impact assessment, data governance)

### Phase 4: Testing

**File: `tests/test_analyzer.py`** (new file to be created)

- [ ] Create test file with pytest structure
- [ ] Add unit tests for `analyze_reverse_lineage()`
- [ ] Add integration tests for CLI

**File: `tests/fixtures/test_reverse_lineage.sql`** (new fixture)

- [ ] Create test query with clear impact relationships

### Phase 5: Error Handling & Edge Cases

- [ ] Source column not found error with helpful message
- [ ] Source column with no dependents (return empty list)
- [ ] Mutual exclusivity validation (--column + --source-column)
- [ ] Table-level reverse lineage validation (column-level only in Phase 1)

## Critical Files to Modify

1. **[analyzer.py](src/sqlglider/lineage/analyzer.py)** - Add `analyze_reverse_lineage()` method (~50 lines)
2. **[cli.py](src/sqlglider/cli.py)** - Add `--source-column` parameter and reverse branch (~30 lines)
3. **[ARCHITECTURE.md](ARCHITECTURE.md)** - Document reverse lineage design (~100 lines)
4. **[CLAUDE.md](CLAUDE.md)** - Update CLI usage (~20 lines)
5. **[README.md](README.md)** - Add user documentation (~50 lines)

## Files NOT Modified

- **`src/sqlglider/lineage/formatters.py`** - No changes needed (direction-agnostic!)
- **`src/sqlglider/utils/file_utils.py`** - No changes needed
- **`pyproject.toml`** - No new dependencies required

## Backward Compatibility Guarantees

1. **Existing CLI behavior unchanged:**
   - `sqlglider lineage query.sql` produces same output as before
   - `--column` flag works identically
   - All existing flags and options preserved

2. **Output format consistency:**
   - Text, JSON, CSV formats unchanged for forward lineage
   - Same formatter code handles both directions

3. **API compatibility:**
   - `LineageAnalyzer.analyze_column_lineage()` signature unchanged
   - New method added, no existing methods modified
   - `ColumnLineage` model unchanged (semantic reuse only)

## Success Criteria

### Functionality
- [ ] Reverse lineage correctly identifies affected outputs
- [ ] Works with simple queries, JOINs, CTEs, complex expressions
- [ ] All output formats (text, JSON, CSV) work correctly

### Backward Compatibility
- [ ] All existing tests pass (when created)
- [ ] Existing CLI behavior unchanged
- [ ] No breaking changes to API

### User Experience
- [ ] Intuitive CLI flag (`--source-column`)
- [ ] Clear error messages
- [ ] Consistent output format with forward lineage

### Code Quality
- [ ] Type hints for all new code
- [ ] Docstrings for new methods
- [ ] Passes ruff linting

## Estimated Effort

**Total: 4-6 hours**

- **Core Implementation:** 2 hours
- **CLI Integration:** 1 hour
- **Documentation:** 1-2 hours
- **Testing:** 1-2 hours
- **Error Handling:** 30 min

## Conclusion

This plan delivers reverse lineage capability through a simple, elegant design that:

- **Maximizes code reuse** - No formatter changes needed
- **Maintains backward compatibility** - Zero breaking changes
- **Provides intuitive UX** - Clear `--source-column` flag
- **Leverages existing robustness** - Builds on proven forward lineage
- **Enables future growth** - Foundation for advanced impact analysis

The key insight is **semantic field reuse**: By interpreting `ColumnLineage` fields differently in reverse mode, we gain all formatter compatibility for free while maintaining a clean, consistent API.
