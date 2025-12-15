# SQL Glider Architecture

## Overview

SQL Glider is a SQL Utility Toolkit built on SQLGlot that provides column-level and table-level lineage analysis for SQL queries. It operates on standalone SQL files without requiring a project framework, making it ideal for ad-hoc query analysis, data governance, and understanding query dependencies.

## Project Structure

```
sql-glider/
├── src/
│   └── sqlglider/
│       ├── __init__.py              # Package initialization
│       ├── cli.py                   # Typer CLI entry point
│       ├── dissection/
│       │   ├── __init__.py          # Dissection module exports
│       │   ├── models.py            # ComponentType, SQLComponent, QueryDissectionResult
│       │   ├── analyzer.py          # DissectionAnalyzer for query decomposition
│       │   └── formatters.py        # Output formatters (text, JSON, CSV)
│       ├── graph/
│       │   ├── __init__.py          # Graph module exports
│       │   ├── models.py            # Pydantic models for graph data
│       │   ├── builder.py           # GraphBuilder for creating graphs from SQL
│       │   ├── merge.py             # GraphMerger for combining graphs
│       │   ├── query.py             # GraphQuerier for upstream/downstream analysis
│       │   └── serialization.py     # JSON save/load, rustworkx conversion
│       ├── lineage/
│       │   ├── __init__.py          # Lineage module exports
│       │   ├── analyzer.py          # Core lineage analysis logic
│       │   └── formatters.py        # Output formatters (text, JSON, CSV)
│       ├── catalog/
│       │   ├── __init__.py          # Catalog module exports
│       │   ├── base.py              # Abstract Catalog class + CatalogError
│       │   ├── registry.py          # Plugin discovery via entry points
│       │   └── databricks.py        # Databricks Unity Catalog implementation
│       ├── templating/
│       │   ├── __init__.py          # Templating module exports
│       │   ├── base.py              # Abstract Templater class + TemplaterError
│       │   ├── registry.py          # Plugin discovery for templaters
│       │   ├── jinja.py             # Jinja2 templater implementation
│       │   └── variables.py         # Variable loading from multiple sources
│       └── utils/
│           ├── __init__.py          # Utils module exports
│           ├── config.py            # Configuration file loading
│           └── file_utils.py        # File I/O utilities
├── tests/
│   ├── __init__.py
│   ├── sqlglider/
│   │   ├── __init__.py
│   │   ├── test_cli.py              # CLI integration tests
│   │   ├── dissection/
│   │   │   ├── __init__.py
│   │   │   ├── test_models.py       # Dissection model tests
│   │   │   ├── test_analyzer.py     # DissectionAnalyzer tests
│   │   │   └── test_formatters.py   # Dissection formatter tests
│   │   ├── graph/
│   │   │   ├── __init__.py
│   │   │   ├── test_models.py       # Graph model tests
│   │   │   ├── test_builder.py      # GraphBuilder tests
│   │   │   ├── test_merge.py        # GraphMerger tests
│   │   │   ├── test_query.py        # GraphQuerier tests
│   │   │   └── test_serialization.py # Serialization tests
│   │   ├── lineage/
│   │   │   ├── __init__.py
│   │   │   ├── test_analyzer.py     # Analyzer unit tests
│   │   │   └── test_formatters.py   # Formatter unit tests
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── test_config.py       # Config unit tests
│   │       └── test_file_utils.py   # File utils tests
│   └── fixtures/
│       ├── sample_queries.sql       # Test SQL files
│       ├── sample_manifest.csv      # Example manifest file
│       └── multi_file_queries/      # Multi-file test fixtures
│           ├── customers.sql
│           ├── orders.sql
│           └── reports.sql
├── main.py                          # Backward compatibility entry point
├── pyproject.toml                   # Project configuration & dependencies
├── sqlglider.toml.example           # Example configuration file
├── ARCHITECTURE.md                  # This file
├── CLAUDE.md                        # Claude Code development guidelines
└── README.md                        # User-facing documentation
```

## Core Components

### 1. CLI Layer (`cli.py`)

**Purpose:** Command-line interface built with Typer

**Key Features:**
- Main entry point: `sqlglider lineage <sql_file>`
- Command structure uses `@app.callback()` to force explicit `lineage` subcommand
- Rich console integration for colored output
- Comprehensive error handling with user-friendly messages

**Command Options:**
- `sql_file` (required): Path to SQL file to analyze
- `--level, -l`: Analysis level (`column` or `table`)
- `--dialect, -d`: SQL dialect (default: `spark`)
- `--column, -c`: Specific output column for forward lineage (optional)
- `--source-column, -s`: Source column for reverse lineage/impact analysis (optional)
- `--output-format, -f`: Output format (`text`, `json`, or `csv`)
- `--output-file, -o`: Export to file instead of stdout (optional)

**Lineage Modes:**
- **Forward Lineage (default):** Find source columns for output columns
  - Use `--column` to analyze a specific output column, or omit for all columns
  - Example: `sqlglider lineage query.sql --column customer_name`
- **Reverse Lineage (impact analysis):** Find output columns affected by a source column
  - Use `--source-column` to specify the source column to analyze
  - Example: `sqlglider lineage query.sql --source-column orders.customer_id`
  - Note: `--column` and `--source-column` are mutually exclusive

**Tables Command Group:** `sqlglider tables <subcommand>`
- `tables overview <sql_file>`: Lists all tables involved in SQL files with usage and type information
- Outputs include: table name (fully qualified), usage (INPUT/OUTPUT/BOTH), object type (TABLE/VIEW/CTE/UNKNOWN)
- Supports all standard options: `--dialect`, `--output-format`, `--output-file`, `--templater`, `--var`, `--vars-file`
- Example: `sqlglider tables overview query.sql --output-format json`

**Error Handling:**
- File not found errors
- SQL parsing errors
- Column not found errors
- Invalid parameter validation
- All errors written to stderr with Rich formatting

### 2. Lineage Analysis (`lineage/analyzer.py`)

**Purpose:** Core lineage analysis using SQLGlot's lineage API

**Data Models (Pydantic):**

```python
class LineageItem(BaseModel):
    """Represents a single lineage relationship (output -> source)."""
    output_name: str  # Output column/table name
    source_name: str  # Source column/table name

class QueryMetadata(BaseModel):
    """Query execution context."""
    query_index: int  # 0-based query index
    query_preview: str  # First 100 chars of query

class QueryLineageResult(BaseModel):
    """Complete lineage result for a single query."""
    metadata: QueryMetadata
    lineage_items: List[LineageItem]  # Flat list of lineage relationships
    level: Literal["column", "table"]
```

**Key Class:**

```python
class LineageAnalyzer:
    def __init__(self, sql: str, dialect: str = "spark")
    def analyze_queries(
        self,
        level: Literal["column", "table"] = "column",
        column: Optional[str] = None,
        source_column: Optional[str] = None,
        table_filter: Optional[str] = None,
    ) -> List[QueryLineageResult]

    # Internal helper methods
    def _iterate_queries(self, table_filter: Optional[str] = None) -> Iterator[Tuple[int, Expression, str]]
    def _analyze_column_lineage_internal(self, column: Optional[str] = None) -> List[LineageItem]
    def _analyze_reverse_lineage_internal(self, source_column: str) -> List[LineageItem]
    def _analyze_table_lineage_internal(self) -> List[LineageItem]
```

**Implementation Details:**

1. **SQL Parsing:**
   - Uses `sqlglot.parse_one(sql, dialect=dialect)` to create AST
   - Handles parsing errors gracefully

2. **Column Extraction:**
   - Traverses `exp.Select.expressions` to find output columns
   - Handles aliased columns (`exp.Alias`)
   - Handles direct column references (`exp.Column`)
   - Falls back to SQL representation for complex expressions

3. **Lineage Tracing:**
   - For each output column, calls `sqlglot.lineage.lineage(column, sql, dialect)`
   - Returns a `Node` tree representing dependencies
   - Recursively traverses tree using `_collect_source_columns()`
   - Leaf nodes (empty `downstream`) are the ultimate source columns

4. **Multi-Level Tracing:**
   - Automatically handles CTEs and subqueries
   - Example: If column A → CTE column B → source column C, returns C
   - Recursive tree traversal ensures all dependency levels are traced

5. **Error Resilience:**
   - If lineage fails for a column (e.g., literals), returns empty sources
   - Continues processing remaining columns

### 3. Output Formatters (`lineage/formatters.py`)

**Purpose:** Format lineage results for different output modes

**Unified Formatter Design:**
Each formatter has a single `format()` method that handles both single and multi-query files, column and table lineage. All outputs include `query_index` for consistency.

**Formatters:**

1. **TextFormatter:**
   ```
   ==========
   Query 0: SELECT ...
   ==========
   ----------
   output_column
   ----------
   source1
   source2
   ```

2. **JsonFormatter:**
   ```json
   {
     "queries": [
       {
         "query_index": 0,
         "query_preview": "SELECT ...",
         "level": "column",
         "lineage": [
           {"output_name": "table.col_a", "source_name": "table.col_x"},
           {"output_name": "table.col_a", "source_name": "table.col_y"}
         ]
       }
     ]
   }
   ```

3. **CsvFormatter:**

   **Column-level:**
   ```csv
   query_index,output_column,source_column
   0,table.column_a,source_table.column_x
   0,table.column_a,source_table.column_y
   ```

   **Table-level:**
   ```csv
   query_index,output_table,source_table
   0,query_result,customers
   0,query_result,orders
   ```

   - Each `LineageItem` becomes one CSV row
   - Multiple sources for same output → multiple rows

4. **OutputWriter:**
   - Writes to file if `output_file` specified
   - Otherwise prints to stdout

**Design Notes:**
- All formatters use static methods for simplicity
- Single `format(results: List[QueryLineageResult])` method per formatter
- Works for both single-query (len=1) and multi-query files transparently
- Level distinction (column vs table) determined from `results[0].level`
- Eliminates code duplication across single/multi-query modes

### 4. Graph Module (`graph/`)

**Purpose:** Cross-file lineage analysis at scale using rustworkx graphs

The graph module enables building, merging, and querying lineage graphs from multiple SQL files. It uses rustworkx (a Rust-based graph library) for high-performance graph operations.

#### Data Models (`graph/models.py`)

```python
class GraphNode(BaseModel):
    """Represents a column as a graph node."""
    identifier: str          # Unique key (e.g., "orders.customer_id")
    file_path: str           # Source file where first encountered
    query_index: int         # Query index within file
    schema_name: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None

    @classmethod
    def from_identifier(cls, identifier: str, file_path: str, query_index: int) -> "GraphNode"

class GraphEdge(BaseModel):
    """Represents a contributes_to relationship."""
    source_node: str         # Source column identifier
    target_node: str         # Target column identifier
    file_path: str           # Where relationship defined
    query_index: int         # Query index

class GraphMetadata(BaseModel):
    """Graph-level metadata."""
    node_format: str = "qualified"
    default_dialect: str = "spark"
    source_files: List[str] = []
    total_nodes: int = 0
    total_edges: int = 0

class LineageGraph(BaseModel):
    """Complete graph with metadata, nodes, and edges."""
    metadata: GraphMetadata
    nodes: List[GraphNode] = []
    edges: List[GraphEdge] = []

class ManifestEntry(BaseModel):
    """Single entry in a manifest file."""
    file_path: Path
    dialect: Optional[str] = None

class Manifest(BaseModel):
    """Collection of manifest entries."""
    entries: List[ManifestEntry] = []

    @classmethod
    def from_csv(cls, path: Path) -> "Manifest"
```

#### Graph Builder (`graph/builder.py`)

```python
class GraphBuilder:
    def __init__(self, dialect: str = "spark", node_format: str = "qualified")

    # Add SQL sources
    def add_file(self, file_path: Path) -> "GraphBuilder"
    def add_files(self, file_paths: List[Path]) -> "GraphBuilder"
    def add_directory(self, dir_path: Path, recursive: bool = True, glob_pattern: str = "*.sql") -> "GraphBuilder"
    def add_manifest(self, manifest_path: Path) -> "GraphBuilder"

    # Build output
    def build(self) -> LineageGraph

    # Access internal graph
    @property
    def rustworkx_graph(self) -> rx.PyDiGraph
    @property
    def node_index_map(self) -> Dict[str, int]
```

**Key Features:**
- Uses `LineageAnalyzer` internally to extract lineage from each SQL file
- Nodes are deduplicated by identifier (first occurrence wins)
- Edges are deduplicated by (source_node, target_node) pair
- Supports method chaining for fluent API

#### Graph Merger (`graph/merge.py`)

```python
class GraphMerger:
    def add_graph(self, graph: LineageGraph) -> "GraphMerger"
    def add_file(self, file_path: Path) -> "GraphMerger"
    def add_files(self, file_paths: List[Path]) -> "GraphMerger"
    def merge(self) -> LineageGraph

def merge_graphs(file_paths: List[Path]) -> LineageGraph
```

**Key Features:**
- Combines multiple graphs into one "mega graph"
- Deduplicates nodes and edges across graphs
- Aggregates and deduplicates source files from all graphs
- Convenience `merge_graphs()` function for simple use cases

#### Graph Querier (`graph/query.py`)

```python
class LineagePath(BaseModel):
    """Represents a path through the lineage graph."""
    nodes: List[str]            # Ordered list of node identifiers

    @property
    def hops(self) -> int       # Number of edges in path (len(nodes) - 1)
    def to_arrow_string(self) -> str  # Format as "a -> b -> c"

class LineageNode(BaseModel):
    """Extended node with query result context."""
    # Fields from GraphNode
    identifier: str
    file_path: str
    query_index: int
    schema_name: Optional[str]
    table: Optional[str]
    column: Optional[str]

    # Query result fields
    hops: int                   # Shortest path distance from queried column
    output_column: str          # The column that was queried
    is_root: bool               # True if node has no upstream dependencies
    is_leaf: bool               # True if node has no downstream dependencies
    paths: List[LineagePath]    # All paths from this node to queried column

class LineageQueryResult:
    query_column: str           # Column that was queried
    direction: str              # "upstream" or "downstream"
    related_columns: List[LineageNode]  # Related columns with paths and root/leaf info

    def __len__(self) -> int
    def __iter__(self) -> Iterator[LineageNode]

class GraphQuerier:
    def __init__(self, graph: LineageGraph)

    @classmethod
    def from_file(cls, file_path: Path) -> "GraphQuerier"

    def find_upstream(self, column: str) -> LineageQueryResult
    def find_downstream(self, column: str) -> LineageQueryResult
    def list_columns(self) -> List[str]
```

**Key Features:**
- Uses `rustworkx.dijkstra_shortest_path_lengths()` for hop counting
- Uses `rustworkx.all_simple_paths()` for path tracking
- Root detection via `in_degree == 0`, leaf detection via `out_degree == 0`
- Case-insensitive column matching
- Results sorted alphabetically by identifier
- All paths from each dependency to the queried column are included

#### Serialization (`graph/serialization.py`)

```python
def save_graph(graph: LineageGraph, path: Path) -> None
def load_graph(path: Path) -> LineageGraph
def to_rustworkx(graph: LineageGraph) -> Tuple[rx.PyDiGraph, Dict[str, int]]
def from_rustworkx(rx_graph: rx.PyDiGraph, metadata: GraphMetadata) -> LineageGraph
```

**Format:** JSON using Pydantic's `model_dump_json()` and `model_validate_json()`

#### CLI Commands

```bash
# Build graph from SQL files
sqlglider graph build query.sql -o graph.json
sqlglider graph build ./queries/ -r -o graph.json
sqlglider graph build --manifest manifest.csv -o graph.json

# Merge multiple graphs
sqlglider graph merge graph1.json graph2.json -o merged.json
sqlglider graph merge --glob "*.json" -o merged.json

# Query lineage
sqlglider graph query graph.json --upstream orders.customer_id
sqlglider graph query graph.json --downstream customers.id -f json
```

### 5. Dissection Module (`dissection/`)

**Purpose:** Decompose SQL queries into constituent parts for unit testing and analysis

The dissection module enables extracting components from SQL queries (CTEs, subqueries, UNION branches, etc.) so they can be tested individually or analyzed for structure.

#### Data Models (`dissection/models.py`)

```python
class ComponentType(str, Enum):
    """Type of SQL component extracted from a query."""
    CTE = "CTE"                     # Common Table Expression
    MAIN_QUERY = "MAIN_QUERY"       # Primary SELECT statement
    SUBQUERY = "SUBQUERY"           # Nested SELECT in FROM clause
    SCALAR_SUBQUERY = "SCALAR_SUBQUERY"  # Single-value subquery
    TARGET_TABLE = "TARGET_TABLE"   # Output table for DML/DDL
    SOURCE_QUERY = "SOURCE_QUERY"   # SELECT within DML/DDL
    UNION_BRANCH = "UNION_BRANCH"   # Individual SELECT in UNION

class SQLComponent(BaseModel):
    """Represents an extracted SQL component."""
    component_type: ComponentType
    component_index: int           # Sequential order within query
    name: Optional[str] = None     # CTE name, alias, or target table
    sql: str                       # Extracted SQL for this component
    parent_index: Optional[int] = None  # Index of parent component
    depth: int = 0                 # Nesting level (0 = top-level)
    is_executable: bool = True     # Can run standalone?
    dependencies: List[str] = []   # CTE names this depends on
    location: str = ""             # Human-readable location context

class QueryMetadata(BaseModel):
    """Metadata about a dissected query."""
    query_index: int               # 0-based index in multi-query file
    query_preview: str             # First 100 chars of query
    statement_type: str            # SELECT, INSERT, CREATE, etc.
    total_components: int          # Number of components extracted

class QueryDissectionResult(BaseModel):
    """Complete dissection result for a single query."""
    metadata: QueryMetadata
    components: List[SQLComponent]
    original_sql: str              # Full original SQL for reference

    def get_component_by_name(self, name: str) -> Optional[SQLComponent]
    def get_components_by_type(self, component_type: ComponentType) -> List[SQLComponent]
    def get_executable_components(self) -> List[SQLComponent]
```

#### Dissection Analyzer (`dissection/analyzer.py`)

```python
class DissectionAnalyzer:
    def __init__(self, sql: str, dialect: str = "spark")
    def dissect_queries(self) -> List[QueryDissectionResult]
```

**Extraction Order:**
1. CTEs (by declaration order)
2. TARGET_TABLE (for INSERT/CREATE/MERGE)
3. SOURCE_QUERY (for DML/DDL statements)
4. MAIN_QUERY (with full SQL including WITH clause)
5. UNION_BRANCHES (if MAIN_QUERY is a UNION)
6. SUBQUERIES (depth-first from FROM clauses)
7. SCALAR_SUBQUERIES (from SELECT list, WHERE, HAVING)

**Key Features:**
- Uses SQLGlot AST traversal for accurate extraction
- Tracks CTE dependencies by finding table references matching CTE names
- UNION flattening extracts all branches from nested UNION expressions
- Parent-child relationships via `parent_index` and `depth`
- `location` field provides human-readable context (e.g., "SELECT list in CTE 'customer_segments'")

#### Formatters (`dissection/formatters.py`)

```python
class DissectionTextFormatter:
    @staticmethod
    def format(results: List[QueryDissectionResult], console: Console) -> None

class DissectionJsonFormatter:
    @staticmethod
    def format(results: List[QueryDissectionResult]) -> str

class DissectionCsvFormatter:
    @staticmethod
    def format(results: List[QueryDissectionResult]) -> str
```

**Output Formats:**
- **Text:** Rich table with columns for Index, Type, Name, Depth, Executable, Location, SQL Preview
- **JSON:** Full structured data with all component details
- **CSV:** Flattened format with semicolon-separated dependencies

#### CLI Command

```bash
# Dissect a SQL file
sqlglider dissect query.sql

# JSON output
sqlglider dissect query.sql --output-format json

# CSV output
sqlglider dissect query.sql --output-format csv

# Export to file
sqlglider dissect query.sql -f json -o dissected.json

# From stdin
echo "WITH cte AS (SELECT id FROM users) SELECT * FROM cte" | sqlglider dissect

# With templating
sqlglider dissect query.sql --templater jinja --var schema=analytics
```

### 6. File Utilities (`utils/file_utils.py`)

**Purpose:** File I/O operations with proper error handling

```python
def read_sql_file(file_path: Path) -> str
```

**Error Handling:**
- FileNotFoundError: File doesn't exist
- ValueError: Path is not a file
- PermissionError: Cannot read file
- UnicodeDecodeError: File not UTF-8 encoded

### 7. Configuration System (`utils/config.py`)

**Purpose:** Load and manage configuration from `sqlglider.toml`

**Data Model (Pydantic):**

```python
class ConfigSettings(BaseModel):
    dialect: Optional[str] = None
    level: Optional[str] = None
    output_format: Optional[str] = None
    templater: Optional[str] = None
    templating: Optional[TemplatingConfig] = None
    catalog_type: Optional[str] = None
    ddl_folder: Optional[str] = None
    catalog: Optional[CatalogConfig] = None
```

**Key Functions:**

```python
def find_config_file(start_path: Optional[Path] = None) -> Optional[Path]
def load_config(config_path: Optional[Path] = None) -> ConfigSettings
```

**Configuration Priority:**
1. CLI arguments (explicit user input)
2. `sqlglider.toml` in current working directory
3. Hardcoded defaults in CLI

**Error Handling:**
- Missing config file: Silently continues with defaults (config is optional)
- Malformed TOML: Warns user to stderr, continues with defaults
- Invalid values: Warns user, uses defaults for invalid fields
- Unknown keys: Ignored for forward compatibility

**Configuration File Format:**

```toml
[sqlglider]
dialect = "postgres"
level = "column"
output_format = "json"
catalog_type = "databricks"
ddl_folder = "./ddl"

[sqlglider.catalog.databricks]
warehouse_id = "abc123..."
```

**Design Notes:**
- Uses Python's built-in `tomllib` (Python 3.11+, zero external dependencies)
- Config is project-specific (PWD only, no user-level config)
- Fail-safe: Never crashes on config errors
- Forward compatible: Ignores unknown settings for future features

### 8. Catalog Module (`catalog/`)

**Purpose:** Plugin system for fetching DDL from remote data catalogs

The catalog module provides an extensible architecture for connecting to various data catalogs (e.g., Databricks Unity Catalog) and fetching table DDL definitions.

**Plugin Architecture:**

```python
# Abstract base class
class Catalog(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def get_ddl(self, table_name: str) -> str: ...

    @abstractmethod
    def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]: ...

    def configure(self, config: Optional[Dict[str, Any]] = None) -> None: ...
```

**Registry Pattern:**

- Catalogs are discovered via Python entry points (`sqlglider.catalogs`)
- Lazy loading with graceful handling of missing optional dependencies
- Factory function `get_catalog(name)` returns configured instances

**Built-in Catalogs:**

- **databricks**: Databricks Unity Catalog via `databricks-sdk`
  - Uses `SHOW CREATE TABLE` via statement execution API
  - Requires warehouse ID for SQL execution
  - Authentication via env vars or config

**CLI Integration:**

```bash
# Pull DDL for tables in a SQL file
sqlglider tables pull query.sql --catalog-type databricks

# Output to folder (one file per table)
sqlglider tables pull query.sql -c databricks -o ./ddl/

# List available catalog providers
sqlglider tables pull --list
```

**Adding Custom Catalogs:**

1. Create a class inheriting from `Catalog`
2. Register via entry point in `pyproject.toml`:
   ```toml
   [project.entry-points."sqlglider.catalogs"]
   my-catalog = "my_package.catalog:MyCatalog"
   ```

## Technology Stack

### Core Dependencies

- **sqlglot[rs] >= 25.0.0:** SQL parser and lineage analysis library with Rust extensions
  - Provides SQL parsing, AST manipulation, and lineage analysis
  - The `[rs]` extra includes Rust-accelerated components for better performance

- **typer >= 0.9.0:** CLI framework with type hints and automatic help generation

- **rich >= 13.0.0:** Terminal formatting and colored output

- **pydantic >= 2.0.0:** Data validation and serialization with type hints

- **rustworkx >= 0.15.0:** High-performance graph library (Rust-based)
  - Used for graph-based lineage analysis
  - Provides efficient `ancestors()` and `descendants()` traversal
  - `PyDiGraph` for directed graph operations

### Development Dependencies

- **ruff >= 0.14.8:** Fast Python linter and formatter

### Python Version

- **Python >= 3.11:** Required for modern type hints and performance

## Key Design Decisions

### 1. SQLGlot for Lineage Analysis

**Decision:** Use SQLGlot directly for SQL parsing and lineage analysis

**Rationale:**
- SQLGlot is a powerful, lightweight SQL parser with built-in lineage capabilities
- SQL Glider targets standalone SQL files for ad-hoc analysis (no project framework needed)
- SQLGlot's lineage API provides everything needed for column-level and table-level tracing
- Simpler, more lightweight implementation compared to full transformation frameworks
- Rust-accelerated components (`[rs]` extra) provide excellent performance

### 2. Pydantic for Data Models

**Decision:** Use Pydantic instead of dataclasses

**Rationale:**
- Built-in JSON serialization via `model_dump()`
- Runtime type validation
- Better IDE support and documentation
- Extensibility for future features (validation, computed fields)

### 3. CLI Structure with Callback

**Decision:** Use `@app.callback()` to force explicit `lineage` subcommand

**Rationale:**
- User requirement: `sqlglider lineage <file>`
- Typer makes single commands the default (would be just `sqlglider <file>`)
- Callback prevents default behavior, making `lineage` explicit
- Allows for future commands to be added easily

### 4. Recursive Lineage Traversal

**Decision:** Recursively traverse SQLGlot's lineage tree to collect leaf nodes

**Rationale:**
- Handles multi-level dependencies automatically (CTEs, subqueries)
- Simple depth-first traversal algorithm
- Leaf nodes represent ultimate source columns
- Matches user requirement for "trace through multiple levels"

### 5. Three Output Formats

**Decision:** Support text, JSON, and CSV

**Rationale:**
- Text: Human-readable, matches user's specified format
- JSON: Machine-readable, structured data for downstream tools
- CSV: Tabular format for spreadsheet analysis
- Covers most common use cases for lineage data

### 6. Reverse Lineage via Graph Inversion

**Decision:** Implement reverse lineage by running forward lineage and inverting the dependency graph

**Rationale:**
- **Leverages existing code:** Reuses forward lineage implementation internally
- **Handles edge cases:** Automatically inherits all SQLGlot edge case handling (CTEs, subqueries, complex expressions)
- **Simple algorithm:** O(n) time complexity with straightforward graph inversion
- **Works with unified model:** Uses same `QueryLineageResult` / `LineageItem` structure as forward lineage
- **Maintainability:** Less code to maintain, consistent data flow
- **Performance:** Negligible overhead even for complex queries (typically <100ms)

**Alternative Considered:**
- Traverse SQLGlot Node tree from root → Rejected because Node doesn't expose parent/upstream references
- Build bidirectional graph → Rejected as over-engineering for current needs
- Separate data models for reverse → Rejected in favor of unified `LineageItem` model

### 7. Configuration File Support

**Decision:** Use TOML for project-level configuration with PWD-based discovery and graceful error handling

**Rationale:**
- **TOML format:** Human-friendly, widely adopted in Python ecosystem (pyproject.toml, Cargo.toml)
- **Built-in library:** Python 3.11+ includes `tomllib` in standard library (zero dependencies)
- **PWD-only:** Config in current working directory matches project-based tools (no user-level config complexity)
- **Graceful degradation:** Config is optional, never blocks execution, warns on errors
- **Clear priority:** CLI > config > defaults prevents confusion and maintains predictability
- **Forward compatible:** Unknown keys ignored, allowing future config options without breaking old configs
- **Precedent for future:** Establishes pattern for application-level defaults with CLI overrides

**Design Pattern:**
```python
# Priority resolution
dialect = cli_arg or config.dialect or "spark"
```

**Alternative Considered:**
- JSON/YAML config → Rejected in favor of TOML (more readable, Python ecosystem standard)
- User-level config (~/.config/sqlglider/) → Rejected to maintain project isolation and simplicity
- Environment variables → Reserved for future enhancement (SQLGLIDER_CONFIG path override)

### 8. Rustworkx for Graph-Based Lineage

**Decision:** Use rustworkx `PyDiGraph` for cross-file lineage analysis

**Rationale:**
- **High Performance:** Rust-based implementation handles thousands of SQL files efficiently
- **Built-in Traversal:** `ancestors()` and `descendants()` provide exactly what's needed for upstream/downstream queries
- **Memory Efficient:** Sparse graph representation minimizes memory usage
- **Python Integration:** Clean Python API despite Rust implementation
- **Precedent:** Consistent with sqlglot[rs] using Rust for performance-critical code

**Graph Structure:**
- **Nodes:** Columns identified by fully qualified name (e.g., "orders.customer_id")
- **Edges:** Directed edges from source column to target column ("contributes_to" relationship)
- **Metadata:** File path and query index for traceability

**Deduplication Strategy:**
- Nodes deduplicated by identifier (first occurrence wins for metadata)
- Edges deduplicated by (source_node, target_node) pair
- Source files aggregated and deduplicated across merged graphs

**Serialization:**
- JSON format using Pydantic's serialization (not rustworkx native serialization)
- Allows for easy inspection and manipulation of graph files
- Conversion functions translate between Pydantic models and rustworkx graph

**Alternative Considered:**
- networkx → Rejected due to Python-only implementation (slower for large graphs)
- rustworkx native serialization → Rejected in favor of readable JSON format
- Custom graph implementation → Rejected as unnecessary complexity

### 9. Unified Single/Multi-Query Processing

**Decision:** Treat all SQL files as multi-query (even single-statement files), use unified data models, and consolidate to one `analyze_queries()` method and one `format()` method per formatter

**Rationale:**
- **Eliminates duplication:** Removed ~200 lines of duplicated code across 6 analyzer methods, 3 formatter classes, and 12 CLI code paths
- **Single source of truth:** One method handles all cases (forward, reverse, column, table, single, multi)
- **Consistent output:** All files show `query_index` (0 for single-query files), making output format predictable
- **Easier maintenance:** Changes only need to be made in one place
- **Simpler API:** Users don't need to know if file has single or multiple queries
- **Query Iterator Pattern:** Centralizes filtering (by table) and preview generation in `_iterate_queries()`
- **Flattened data model:** `LineageItem` represents individual output→source relationships, simplifying CSV export

**Implementation Details:**
- Single-query files produce `List[QueryLineageResult]` with length 1 and `query_index=0`
- Multi-query files produce one `QueryLineageResult` per query with sequential indices
- CLI simplified from 12 code paths to 3 (based on output format only)
- Formatters work identically for single and multi-query files
- `table_filter` parameter allows filtering multi-query files to specific tables

**Benefits:**
- **Code size:** Reduced from 848 to 749 lines (analyzer), 399 to 192 lines (formatters)
- **Consistency:** Single-query and multi-query outputs have identical structure
- **Extensibility:** Easy to add new features (e.g., query filtering) that work for all cases
- **Testing:** Simpler test suite with fewer code paths to cover

**Alternative Considered:**
- Maintain separate single/multi methods → Rejected due to ongoing duplication and maintenance burden
- Detect single vs multi and branch → Rejected as unnecessary complexity when unified approach works for both

## SQL Dialect Support

SQLGlot supports many SQL dialects out of the box:

- **spark** (default for SQL Glider)
- **postgres**
- **snowflake**
- **bigquery**
- **redshift**
- **mysql**
- **tsql** (SQL Server)
- **oracle**
- **presto**
- **trino**
- And many more...

Users can specify any SQLGlot-supported dialect via `--dialect` flag.

## Lineage Analysis Algorithm

### Unified Query Processing

The `analyze_queries()` method provides a unified interface for all lineage analysis modes:

1. **Parse SQL:** Parse all statements using `sqlglot.parse()` (supports multi-statement SQL)
2. **Iterate Queries:** Use `_iterate_queries()` to process each query:
   - Generate query preview (first 100 chars)
   - Apply table filter if specified
   - Yield (query_index, expression, preview) tuples
3. **Analyze Each Query:** Temporarily swap `self.expr` and call appropriate internal method:
   - **Column forward:** `_analyze_column_lineage_internal(column)`
   - **Column reverse:** `_analyze_reverse_lineage_internal(source_column)`
   - **Table-level:** `_analyze_table_lineage_internal()`
4. **Build Results:** Create `QueryLineageResult` for each query with:
   - `QueryMetadata` (index and preview)
   - `List[LineageItem]` (flattened output→source relationships)
   - `level` ("column" or "table")
5. **Validate:** If `column` or `source_column` specified but no results found, raise `ValueError`
6. **Return:** `List[QueryLineageResult]` (one per query)

### Column-Level Forward Lineage (Internal)

The `_analyze_column_lineage_internal()` method:

1. **Extract Output Columns:** Traverse SELECT expressions to find all output columns
2. **Filter Columns:** If `column` parameter specified, only analyze that column
3. **For Each Output Column:**
   - Call `sqlglot.lineage.lineage(column, sql, dialect)`
   - Receive a `Node` tree with `downstream` references
   - Recursively traverse tree depth-first
   - Collect leaf nodes (no downstream) as source columns
4. **Flatten Results:** Create `LineageItem` for each output→source relationship
5. **Return:** `List[LineageItem]` (each represents one output→source pair)

### Column-Level Reverse Lineage (Internal)

The `_analyze_reverse_lineage_internal()` method:

1. **Run Forward Lineage:** Call `_analyze_column_lineage_internal()` for all output columns
2. **Build Reverse Map:** Invert the dependency graph
   - For each `LineageItem` (output → source)
   - Create reverse mapping (source → [outputs])
   - Example: If `customer_id` comes from `orders.customer_id`, map `orders.customer_id` → `customer_id`
3. **Find Affected Outputs:** Look up the `source_column` in reverse map
4. **Flatten Results:** Create `LineageItem` for each source→output relationship
   - Note: Semantics inverted - `output_name` is the source, `source_name` is the affected output
5. **Return:** `List[LineageItem]` (empty if source_column not found in this query)

**Algorithm Complexity:**
- Time: O(n + n*m) where n = output columns, m = avg sources per column
- Space: O(n*m) for the reverse mapping dictionary
- Performance: Negligible overhead (<100ms even for complex queries with 50+ columns)

### Table-Level Lineage (Internal)

The `_analyze_table_lineage_internal()` method:

1. **Parse Query:** Use current `self.expr`
2. **Find All Tables:** Search for `exp.Table` nodes in AST
3. **Collect Table Names:** Get fully qualified table names
4. **Create Results:** Generate `LineageItem` for each source table
   - `output_name` = "query_result" (or target table for INSERT/CREATE)
   - `source_name` = source table name
5. **Return:** `List[LineageItem]`

### Multi-Level Example

```sql
WITH order_totals AS (
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
)
SELECT total FROM order_totals
```

Lineage for `total`:
- `total` → `order_totals.total` → `orders.amount`
- Final result: `['orders.amount']`

The recursive traversal automatically traces through the CTE.

## Error Handling Strategy

### Input Validation
- Validate file existence before reading
- Validate SQL dialect support
- Validate output format options
- Validate level options (column/table)

### Parsing Errors
- Catch `sqlglot.errors.ParseError`
- Display user-friendly message with error details
- Exit with code 1

### Runtime Errors
- File I/O errors: Clear messages about permissions, encoding
- Column not found: List available columns to help user
- Lineage failures: Continue processing, return empty sources

### Error Output
- All errors written to stderr (via `console.print(..., stderr=True)`)
- Colored formatting: `[red]Error:[/red]` prefix
- Rich console formatting for better readability

## Future Enhancements

### Potential Features

1. **Schema Support:**
   - `--schema` option to provide table schemas
   - Improves lineage accuracy for complex queries
   - Better handling of `SELECT *`

2. **Table-Level Reverse Lineage:**
   - `--source-table` option for impact analysis at table level
   - Find which output tables depend on a source table
   - Complement to existing column-level reverse lineage

3. **Visualization:**
   - Generate HTML lineage graphs
   - Use SQLGlot's built-in visualization
   - Interactive exploration of dependencies

4. **Batch Processing:**
   - Analyze multiple SQL files at once
   - Aggregate lineage across files
   - Project-level lineage analysis

5. **Configuration File:**
   - `.sqlglider.toml` for default options
   - Per-project dialect settings
   - Custom output formatting

6. **Query Validation:**
   - Validate queries against schemas
   - Detect broken lineage (missing tables/columns)
   - Integration with data catalogs

7. **Performance Optimization:**
   - Cache parsed ASTs for repeated analysis
   - Parallel processing for batch mode
   - Incremental lineage updates

## Testing Strategy

### Unit Tests (To Be Implemented)

**test_analyzer.py:**
- Simple SELECT statements
- JOIN queries
- CTEs (WITH clauses)
- Nested subqueries
- Column aliases
- Complex expressions (CASE, functions)
- Invalid SQL handling
- Column filtering

**test_formatters.py:**
- Text formatting
- JSON formatting
- CSV formatting
- Table lineage formatting
- Edge cases (empty sources, special characters)

**test_file_utils.py:**
- File reading
- Error handling (missing file, permissions, encoding)

### Integration Tests

- End-to-end CLI testing with fixture SQL files
- Different dialect support
- Output file creation
- Error message validation

### Test Fixtures

Create `tests/fixtures/` with various SQL patterns:
- `simple_select.sql`
- `join_query.sql`
- `cte_query.sql`
- `nested_query.sql`
- `invalid_syntax.sql`

## Maintenance Guidelines

### When Adding New Features

1. **Update This Document:** Document architecture changes
2. **Update CLAUDE.md:** Add development guidelines if needed
3. **Update README.md:** Document user-facing changes
4. **Add Tests:** Unit and integration tests for new functionality
5. **Update Type Hints:** Maintain type safety
6. **Run Linter:** `uv run ruff check --fix && uv run ruff format`

### Code Style

- Follow PEP 8 conventions
- Use type hints for all functions
- Pydantic models for data structures
- Docstrings for all public APIs
- Keep functions focused and small
- Prefer composition over inheritance

### Dependency Management

- Use `uv add <package>` to add dependencies
- Use `uv add --dev <package>` for dev dependencies
- Keep dependencies minimal
- Document why each dependency is needed

## Performance Considerations

### Current Performance

- Parsing: SQLGlot is fast (Rust-accelerated in sqlmesh[rs])
- Lineage: O(n) where n = number of nodes in AST
- Memory: Loads entire SQL file into memory (fine for typical queries)

### Potential Bottlenecks

- Very large SQL files (>10MB): Consider streaming parsing
- Complex queries with deep nesting: Recursive traversal depth
- Batch processing: Sequential processing of files

### Optimization Strategies

- Use Rust extensions where available
- Cache parsed ASTs if analyzing same query multiple times
- Parallel processing for batch operations
- Lazy evaluation of lineage (only analyze requested columns)

## Security Considerations

### Input Validation

- SQL files are parsed, not executed
- No SQL injection risk (static analysis only)
- File path validation prevents directory traversal

### Output Sanitization

- No user input in output (only parsed SQL)
- JSON/CSV formatters escape special characters automatically
- File writes use safe Path API

### Dependencies

- All dependencies from trusted sources (PyPI)
- Regular updates for security patches
- Minimal dependency surface area

## Conclusion

SQL Glider provides a lightweight, flexible solution for SQL lineage analysis. By leveraging SQLGlot's powerful parsing and lineage capabilities, it delivers accurate multi-level dependency tracking without the overhead of a full project framework. The modular architecture makes it easy to extend with new features while maintaining simplicity and reliability.
