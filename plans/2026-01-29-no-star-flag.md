# Add `--no-star` Flag

**Status:** Completed

## Overview
Add `--no-star` flag to `lineage` and `graph build` commands. When set, analysis fails if `SELECT *` or `t.*` cannot be resolved to actual columns.

## Changes

### 1. `src/sqlglider/utils/config.py` — Add to ConfigSettings
- [x] Add `no_star: Optional[bool] = None`

### 2. `src/sqlglider/lineage/analyzer.py` — Add parameter + enforce
- [x] Add `no_star: bool = False` to `__init__`, store as `self._no_star`
- [x] Add `StarResolutionError` exception class (distinct from `ValueError` to avoid being swallowed by skipped-query handler)
- [x] DML/DDL path: raise `StarResolutionError` before fallback for bare `*` and `t.*`
- [x] DQL path: add star handling for both bare `*` and `t.*` with same error behavior
- [x] Re-raise `StarResolutionError` in `analyze_queries` instead of treating as skipped query

### 3. `src/sqlglider/graph/builder.py` — Pass through
- [x] Add `no_star: bool = False` to `__init__`, store as `self.no_star`
- [x] Pass to `LineageAnalyzer(sql_content, dialect=file_dialect, no_star=self.no_star)`

### 4. `src/sqlglider/cli.py` — Add CLI options
- [x] `lineage` command: Add `no_star: bool = typer.Option(False, "--no-star", ...)`
- [x] Resolve: `no_star = no_star or config.no_star or False`
- [x] Pass to `LineageAnalyzer(sql, dialect=dialect, no_star=no_star)`
- [x] `graph_build` command: same option, passed to `GraphBuilder(..., no_star=no_star)`

### 5. `tests/sqlglider/lineage/test_analyzer.py` — Tests
- [x] Test bare `SELECT *` with `no_star=True` raises `StarResolutionError`
- [x] Test `SELECT t.*` with `no_star=True` raises `StarResolutionError`
- [x] Test resolvable star (via CTE) still works with `no_star=True`
- [x] Test resolvable qualified star (via CTE) still works with `no_star=True`
- [x] Test default (`no_star=False`) still falls back to `table.*`

## Implementation Notes

### Deviations from original plan
- Used `StarResolutionError` instead of `ValueError` because `analyze_queries` catches `ValueError` to handle unsupported statement types (skipped queries). A plain `ValueError` would be silently swallowed.
- Added star handling in the DQL (plain SELECT) code path in addition to the DML/DDL path. The original plan only addressed the DML/DDL path, but plain `SELECT *` queries go through a different branch in `get_output_columns`.
- Resolvable star tests use CTEs instead of `CREATE TABLE` with explicit columns, since `_extract_schema_from_statement` only handles `CREATE ... AS SELECT`, not DDL with column definitions.

## Verification
- `uv run pytest` — 597 passed, 1 skipped, coverage 80.48%
- `uv run basedpyright src/` — 0 errors
- `uv run ruff check` — all checks passed
