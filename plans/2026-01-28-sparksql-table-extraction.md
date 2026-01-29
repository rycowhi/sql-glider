# SparkSQL Table Extraction for Unsupported Statement Types

**Status:** Planned
**Date:** 2026-01-28

## Overview

The `analyze_tables()` method in `LineageAnalyzer` currently extracts tables from a subset of statement types (SELECT, INSERT, CREATE, DELETE, DROP, TRUNCATE, CACHE). Several SparkSQL-specific statement types reference tables but are not captured during table extraction. This plan adds table extraction support for these missing types.

Column lineage is **not affected** — these statements contain no SELECT and cannot produce column-level lineage. The goal is to ensure `sqlglider tables overview` reports all tables referenced in a SQL file.

## Statements to Add

| Statement | SQLGlot Expression | Table Location | Proposed Usage |
|-----------|-------------------|----------------|----------------|
| `UNCACHE TABLE t` | `exp.Uncache` | `expr.this` | `INPUT` |
| `REFRESH TABLE t` | `exp.Refresh` | `expr.this` | `INPUT` |
| `LOAD DATA INPATH '...' INTO TABLE t` | `exp.LoadData` | `expr.this` | `OUTPUT` |
| `ALTER TABLE t ...` | `exp.Alter` | `expr.this` | `OUTPUT` |
| `ANALYZE TABLE t COMPUTE STATISTICS` | `exp.Analyze` | `expr.this` | `INPUT` |

### Usage Rationale

- **UNCACHE / REFRESH / ANALYZE**: Read-oriented metadata operations on an existing table → `INPUT`
- **LOAD DATA**: Writes data into a table → `OUTPUT`
- **ALTER TABLE**: Modifies table structure → `OUTPUT`

## Implementation Steps

- [ ] Add extraction logic to `_get_target_table_info()` in [analyzer.py](src/sqlglider/lineage/analyzer.py) for each new expression type
- [ ] Add each type to the `_get_statement_type()` type_map for readable skip messages
- [ ] Add entries to `_is_target_table()` where applicable (LOAD DATA, ALTER)
- [ ] Ensure `_get_target_and_select()` returns `None` gracefully for these types (they have no SELECT)
- [ ] Add unit tests in [test_analyzer.py](tests/sqlglider/lineage/test_analyzer.py):
  - Table extraction returns correct table name and usage for each type
  - Column lineage correctly skips these with appropriate message
  - Parameterized test covering all five statement types
- [ ] Verify graph build handles these gracefully (skipped queries warning)
- [ ] Run full test suite and coverage check

## Files to Modify

- `src/sqlglider/lineage/analyzer.py` — extraction logic
- `tests/sqlglider/lineage/test_analyzer.py` — unit tests

## Testing Strategy

- Parameterized tests with SparkSQL syntax for each statement type
- Verify `analyze_tables()` returns correct table name, usage, and object type
- Verify `analyze_queries()` adds these to `skipped_queries` with clear reason
- Ensure no regressions in existing tests
- Coverage threshold (80%) maintained

## Notes

- These are all parsed by sqlglot's Spark dialect parser, so no custom parsing is needed
- Some of these (SHOW, DESCRIBE, EXPLAIN) parse as `exp.Command` — those are intentionally excluded since they don't reference tables in a structured way
- INSERT OVERWRITE and multi-INSERT patterns may warrant separate investigation
