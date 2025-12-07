# Graph-Based Lineage Feature for SQL Glider

**Status:** Completed
**Date:** 2025-12-06
**Completed:** 2025-12-06

## Overview

Add a graph-based lineage feature using [rustworkx](https://www.rustworkx.org/) that enables building, merging, and querying lineage graphs from SQL files. This will allow cross-query lineage analysis at scale (thousands of SQL files).

## CLI Commands

### 1. `graph build` - Build lineage graph from SQL files

```bash
# Single file
sqlglider graph build query.sql -o graph.json

# Multiple files
sqlglider graph build query1.sql query2.sql -o graph.json

# Directory (recursive with glob)
sqlglider graph build ./queries/ -r -g "*.sql" -o graph.json

# From manifest CSV
sqlglider graph build --manifest manifest.csv -o graph.json

# With options
sqlglider graph build ./queries/ -r -o graph.json --dialect postgres --node-format structured
```

**Options:**
- `paths` (argument): SQL file(s), directory, or space-separated paths
- `-o, --output` (required): Output JSON file path
- `-r, --recursive`: Recursively search directories
- `-g, --glob`: Glob pattern for SQL files (default: `*.sql`)
- `-m, --manifest`: Path to manifest CSV file
- `-d, --dialect`: SQL dialect (default: spark)
- `-n, --node-format`: `qualified` (default) or `structured`

### 2. `graph merge` - Combine multiple graphs

```bash
# Multiple files
sqlglider graph merge graph1.json graph2.json -o merged.json

# With glob pattern
sqlglider graph merge --glob "graphs/*.json" -o merged.json
```

**Options:**
- `inputs` (argument): JSON graph files to merge
- `-o, --output` (required): Output file path
- `-g, --glob`: Glob pattern for graph files

### 3. `graph query` - Query upstream/downstream dependencies

```bash
# Find all source columns for a target
sqlglider graph query graph.json --upstream orders.customer_id

# Find all affected columns from a source
sqlglider graph query graph.json --downstream customers.customer_id

# JSON output
sqlglider graph query graph.json --upstream orders.total -f json
```

**Options:**
- `graph_file` (argument): Path to graph JSON file
- `-u, --upstream`: Find source columns that contribute to this column
- `-d, --downstream`: Find columns affected by this source column
- `-f, --output-format`: `text` (default), `json`, or `csv`

## Node Format Options

**Qualified (default):** Simple string identifier
```json
{"identifier": "orders.customer_id", "table": "orders", "column": "customer_id", ...}
```

**Structured:** Separated components for flexible querying
```json
{"identifier": "orders.customer_id", "schema": null, "table": "orders", "column": "customer_id", ...}
```

## Manifest File Format

CSV file with columns: `file_path`, `dialect` (optional)

```csv
file_path,dialect
queries/orders.sql,spark
queries/customers.sql,postgres
queries/legacy.sql,
```

- Empty dialect uses CLI option or default (spark)
- Paths are relative to manifest file location

## Files to Create

### New Module: `src/sqlglider/graph/`

| File | Purpose |
|------|---------|
| `__init__.py` | Export graph module components |
| `models.py` | Pydantic models: `GraphNode`, `GraphEdge`, `GraphMetadata`, `LineageGraph`, `Manifest` |
| `builder.py` | `GraphBuilder` class for creating graphs from SQL files |
| `merge.py` | `GraphMerger` class for combining graphs |
| `query.py` | `GraphQuerier` class for upstream/downstream analysis |
| `serialization.py` | JSON save/load using Pydantic + rustworkx conversion |

### Tests: `tests/sqlglider/graph/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package init |
| `test_models.py` | Pydantic model validation, manifest loading |
| `test_builder.py` | Single file, directory, manifest, multi-file processing |
| `test_merge.py` | Node/edge deduplication, multi-graph merge |
| `test_query.py` | Upstream/downstream queries, case-insensitive matching |
| `test_serialization.py` | Save/load roundtrip, rustworkx conversion |

### Test Fixtures: `tests/fixtures/`

| File | Purpose |
|------|---------|
| `multi_file_queries/customers.sql` | Customer table query |
| `multi_file_queries/orders.sql` | Orders table query |
| `multi_file_queries/reports.sql` | Join query across tables |
| `sample_manifest.csv` | Example manifest file |

## Files to Modify

| File | Changes |
|------|---------|
| `pyproject.toml` | Add `rustworkx>=0.15.0` dependency |
| `src/sqlglider/cli.py` | Add `graph` command group with `build`, `merge`, `query` subcommands |
| `ARCHITECTURE.md` | Document graph module |
| `README.md` | Add graph command examples |
| `CLAUDE.md` | Add graph CLI usage |

## Implementation Phases

### Phase 1: Foundation
- [x] Add rustworkx to `pyproject.toml`
- [x] Create `src/sqlglider/graph/` module structure
- [x] Implement Pydantic models in `models.py`
- [x] Implement serialization in `serialization.py`
- [x] Write tests for models and serialization

### Phase 2: Graph Builder
- [x] Implement `GraphBuilder` class in `builder.py`
- [x] Add single file processing (reuse `LineageAnalyzer`)
- [x] Add directory processing with glob/recursive support
- [x] Add manifest CSV processing
- [x] Add multiple file paths support
- [x] Write builder tests

### Phase 3: Graph Merge
- [x] Implement `GraphMerger` class in `merge.py`
- [x] Add node deduplication by identifier
- [x] Add edge deduplication by (source, target) pair
- [x] Add glob pattern support for input files
- [x] Write merge tests

### Phase 4: Graph Query
- [x] Implement `GraphQuerier` class in `query.py`
- [x] Implement `find_upstream()` using `rx.ancestors()`
- [x] Implement `find_downstream()` using `rx.descendants()`
- [x] Add case-insensitive column matching
- [x] Write query tests

### Phase 5: CLI Integration
- [x] Add `graph` Typer sub-app to `cli.py`
- [x] Implement `graph build` command
- [x] Implement `graph merge` command
- [x] Implement `graph query` command
- [x] Add output formatters for query results
- [x] Write CLI integration tests

### Phase 6: Documentation
- [x] Update ARCHITECTURE.md
- [x] Update README.md
- [x] Update CLAUDE.md
- [x] Create example manifest file

## Key Design Decisions

1. **rustworkx over networkx**: Higher performance for large graphs (Rust-based)
2. **PyDiGraph**: Directed graph for source → target edge semantics
3. **Pydantic models for serialization**: Consistent with existing codebase, easy JSON export
4. **Node deduplication by identifier**: Same column from multiple files shares one node
5. **Edge metadata includes source file**: Traceability to original SQL file
6. **Case-insensitive column matching**: Consistent with existing `LineageAnalyzer` behavior

## Graph Structure

**Nodes** (columns):
```python
GraphNode(
    identifier="orders.customer_id",  # Unique key
    file_path="/path/to/query.sql",   # First occurrence
    query_index=0,                     # Query index in file
    schema_name=None,                  # Optional (structured mode)
    table="orders",                    # Parsed from identifier
    column="customer_id",              # Parsed from identifier
)
```

**Edges** (contributes_to relationships):
```python
GraphEdge(
    source_node="customers.customer_id",  # Source column
    target_node="orders.customer_id",      # Target column
    file_path="/path/to/query.sql",        # Where relationship defined
    query_index=0,                          # Query index
)
```

## Testing Strategy

- Unit tests for each module (models, builder, merge, query, serialization)
- CLI integration tests using `CliRunner`
- Test fixtures with multi-file SQL scenarios
- Maintain 80%+ code coverage
- Parameterized tests for edge cases

**Results:** All 269 tests pass with 86.58% coverage.

## Implementation Notes

### Test Fixes During Implementation
1. **`test_merge_two_graphs`**: Edges referenced non-existent source nodes. Fixed by adding source nodes to the test's node lists.
2. **`test_save_creates_file`** (Windows-specific): `PermissionError` when unlinking temp file while still open. Fixed by using `TemporaryDirectory` instead of `NamedTemporaryFile`.
3. **CLI error tests**: Error messages written to stderr via `console.print(..., err=True)` weren't visible in `result.stdout`. Fixed by checking `result.output` instead.

### Minor Deviations from Plan
- Implemented JSON serialization using Pydantic's native `model_dump_json()` and `model_validate_json()` rather than custom rustworkx serialization, as this produces more readable and maintainable output.

## Sources

- [rustworkx Documentation](https://www.rustworkx.org/)
- [rustworkx Serialization API](https://www.rustworkx.org/api/serialization.html)
- [rustworkx.ancestors](https://www.rustworkx.org/apiref/rustworkx.ancestors.html)
- [rustworkx.descendants](https://www.rustworkx.org/apiref/rustworkx.descendants.html)
