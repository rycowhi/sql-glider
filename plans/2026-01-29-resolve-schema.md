# `--resolve-schema` Flag and Catalog Integration

**Status:** Completed

## Overview

Add `--resolve-schema` flag to `graph build` that runs a two-pass process: first extracting schema from all files, then running lineage analysis with the full schema available. Optionally, `--catalog-type` fills schema gaps by pulling DDL from a remote catalog.

## Design Decisions

- **Types are not required** — SQLGlot only needs column names for star expansion; types are stored as `"UNKNOWN"`
- **Two-pass approach** — Pass 1 extracts schema from all files (order-independent), Pass 2 runs lineage with full schema
- **Opt-in via `--resolve-schema`** — default behavior unchanged
- **Catalog fills gaps only** — file-derived schema always wins over catalog-sourced schema
- **`--catalog-type` requires `--resolve-schema`** — validated at CLI level

## Implementation

- [x] Add `schema` param to `LineageAnalyzer.__init__()` — pre-populates `_file_schema`
- [x] Add `extract_schema_only()` and `get_extracted_schema()` methods to `LineageAnalyzer`
- [x] Create `src/sqlglider/utils/schema.py` with `parse_ddl_to_schema()` for DDL column extraction
- [x] Add `resolve_schema`, `catalog_type`, `catalog_config` to `GraphBuilder`
- [x] Implement `_extract_schemas()` for pass 1 and `_fill_schema_from_catalog()` for catalog gap-filling
- [x] Two-pass flow in `add_files()` and `add_manifest()`
- [x] Add `--resolve-schema` and `--catalog-type` CLI flags to `graph build`
- [x] Add `resolve_schema` to `ConfigSettings`
- [x] Tests: 25 new tests (schema parsing, analyzer schema param, cross-file resolution, catalog mocking)

## Files Modified

- `src/sqlglider/lineage/analyzer.py` — schema param, extraction methods
- `src/sqlglider/graph/builder.py` — two-pass processing, catalog integration
- `src/sqlglider/cli.py` — CLI flags
- `src/sqlglider/utils/config.py` — config setting
- `src/sqlglider/utils/schema.py` — **new** DDL parsing utility
- `tests/sqlglider/utils/test_schema.py` — **new**
- `tests/sqlglider/graph/test_builder.py` — resolve schema + catalog tests
- `tests/sqlglider/lineage/test_analyzer.py` — schema param tests

## Verification

- 617 passed, 1 skipped
- Coverage: 80.43%
- basedpyright: 0 errors
- ruff: all checks passed

## Known Limitations

- Cross-file CTAS chains with `SELECT *` (view B depends on view A via star) may not resolve if both are in separate files and the schema extraction pass processes B before A. This is rare in practice.
