**Status:** Completed

# Plan: `tables scrape` Command

## Overview

Add a `tables scrape` subcommand that performs schema inference (the same logic `graph build --resolve-schema` uses) but outputs the inferred schema directly instead of building a lineage graph. This makes schema inference a standalone, reusable operation.

## Key Changes

### 1. Refactor schema extraction out of GraphBuilder

**File:** `src/sqlglider/graph/builder.py`

Extract `_extract_schemas()` and `_fill_schema_from_catalog()` into a standalone module so both `graph build` and `tables scrape` can use them without instantiating a full `GraphBuilder`.

**New file:** `src/sqlglider/schema/extractor.py`
- `extract_schemas_from_files(file_paths, dialect, sql_preprocessor, schema, strict_schema, console) -> SchemaDict` — core extraction loop with Rich progress bar
- `fill_schema_from_catalog(schema, file_paths, dialect, sql_preprocessor, catalog_type, catalog_config, console) -> SchemaDict` — catalog fill logic
- `extract_and_resolve_schema(file_paths, dialect, sql_preprocessor, strict_schema, catalog_type, catalog_config, console) -> SchemaDict` — high-level orchestrator (extract + optional catalog fill)

**Update GraphBuilder** to delegate to these new functions instead of implementing them inline. `GraphBuilder._extract_schemas` and `_fill_schema_from_catalog` become thin wrappers or are removed, with `extract_schemas()` calling the shared code.

### 2. Add `tables scrape` CLI command

**File:** `src/sqlglider/cli.py`

Add `@tables_app.command("scrape")` with these parameters (mirroring `graph build`):

| Parameter | Source |
|-----------|--------|
| `paths` | Same as `graph build` — file(s) or directory(ies) |
| `--recursive / -r` | Same recursive directory traversal |
| `--glob / -g` | Same glob pattern (default `*.sql`) |
| `--manifest / -m` | Same manifest CSV support |
| `--dialect / -d` | SQL dialect |
| `--templater / -t` | Templater name |
| `--var / -v` | Template variables |
| `--vars-file` | Variables file |
| `--strict-schema` | Strict schema mode |
| `--catalog-type / -c` | Catalog provider for remote DDL |
| `--output-format / -f` | `text` (default), `json`, or `csv` |
| `--output-file / -o` | Output file path (stdout if omitted) |

**Flow:**
1. Resolve config defaults (same pattern as `graph build`)
2. Set up templating preprocessor (same shared code)
3. Collect files from paths/manifest (same logic as `graph build`)
4. Call `extract_and_resolve_schema(...)` from the new shared module
5. Format output using existing `format_schema()` from `src/sqlglider/graph/formatters.py`
6. Write to file or stdout using `OutputWriter`

### 3. Move schema formatters

The existing formatters in `src/sqlglider/graph/formatters.py` are already generic (they format `SchemaDict`). **Decision: leave them in place** to minimize churn — they work for both `graph build --dump-schema` and `tables scrape`.

### 4. Refactor shared file-collection logic

The file collection code (paths + recursive glob + manifest) is duplicated between `graph build` and the new `tables scrape`. Extract a helper function:

```python
def _collect_sql_files(
    paths: Optional[List[Path]],
    manifest: Optional[Path],
    recursive: bool,
    glob_pattern: str,
) -> tuple[list[Path], list[Path]]:
    """Returns (manifest_files, path_files)."""
```

Place this in `cli.py` as a private helper used by both commands.

## Implementation Steps

- [x] Create `src/sqlglider/schema/__init__.py`
- [x] Create `src/sqlglider/schema/extractor.py` with shared schema extraction logic
- [x] Update `src/sqlglider/graph/builder.py` to delegate to shared extractor
- [x] Add `_collect_sql_files` helper to `src/sqlglider/cli.py`
- [x] Refactor `graph build` to use `_collect_sql_files`
- [x] Add `tables scrape` command to `src/sqlglider/cli.py`
- [x] Create `tests/sqlglider/schema/__init__.py`
- [x] Create `tests/sqlglider/schema/test_extractor.py` (10 tests)
- [x] Add `TestTablesScrapeCommand` to `tests/sqlglider/test_cli.py` (11 tests)
- [x] All 672 tests pass, 81.5% coverage, ruff clean

## Files Created/Modified

| File | Action |
|------|--------|
| `src/sqlglider/schema/__init__.py` | Created — empty |
| `src/sqlglider/schema/extractor.py` | Created — shared schema extraction logic |
| `src/sqlglider/graph/builder.py` | Modified — delegate to shared extractor |
| `src/sqlglider/cli.py` | Modified — add `tables scrape` command + `_collect_sql_files` helper |
| `tests/sqlglider/schema/__init__.py` | Created — empty |
| `tests/sqlglider/schema/test_extractor.py` | Created — tests for shared extractor |
| `tests/sqlglider/test_cli.py` | Modified — add tests for `tables scrape` command |

## Testing Strategy

1. **Unit tests for `schema/extractor.py`**: Test `extract_schemas_from_files` with CREATE VIEW/TABLE AS SELECT and DQL qualified refs
2. **CLI tests for `tables scrape`**: Use `CliRunner` to test text/json/csv output, recursive glob, templating, error cases
3. **Regression**: Full test suite passes (672 tests), coverage at 81.5%

## Verification

```bash
# Basic usage
uv run sqlglider tables scrape ./queries/ -r

# With output format
uv run sqlglider tables scrape ./queries/ -r -f json -o schema.json

# With catalog
uv run sqlglider tables scrape ./queries/ -r -c databricks -f csv

# Ensure graph build still works
uv run sqlglider graph build ./queries/ -r --resolve-schema --dump-schema schema.txt -o graph.json
```
