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
│       ├── lineage/
│       │   ├── __init__.py          # Lineage module exports
│       │   ├── analyzer.py          # Core lineage analysis logic
│       │   └── formatters.py        # Output formatters (text, JSON, CSV)
│       └── utils/
│           ├── __init__.py          # Utils module exports
│           ├── config.py            # Configuration file loading
│           └── file_utils.py        # File I/O utilities
├── tests/
│   ├── __init__.py
│   ├── sqlglider/
│   │   ├── __init__.py
│   │   ├── test_cli.py              # CLI integration tests
│   │   ├── lineage/
│   │   │   ├── __init__.py
│   │   │   ├── test_analyzer.py     # Analyzer unit tests
│   │   │   └── test_formatters.py   # Formatter unit tests
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── test_config.py       # Config unit tests
│   │       └── test_file_utils.py   # File utils tests
│   └── fixtures/
│       └── sample_queries.sql       # Test SQL files
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
class ColumnLineage(BaseModel):
    output_column: str
    source_columns: List[str]  # Fully qualified: table.column

class TableLineage(BaseModel):
    output_table: str
    source_tables: List[str]
```

**Key Class:**

```python
class LineageAnalyzer:
    def __init__(self, sql: str, dialect: str = "spark")
    def get_output_columns(self) -> List[str]
    def analyze_column_lineage(self, column: Optional[str] = None) -> List[ColumnLineage]
    def analyze_reverse_lineage(self, source_column: str) -> List[ColumnLineage]
    def analyze_table_lineage(self) -> TableLineage
    def _collect_source_columns(self, node: Node, sources: Set[str]) -> None
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

**Formatters:**

1. **TextFormatter:**
   ```
   ----------
   output_column
   ----------
   source1
   source2
   ```

2. **JsonFormatter:**
   ```json
   {
     "columns": [
       {
         "output_column": "columnA",
         "source_columns": ["table.col1", "table.col2"]
       }
     ]
   }
   ```

3. **CsvFormatter:**
   ```csv
   output_column,source_table,source_column
   columnA,table1,col1
   columnA,table1,col2
   ```
   - Parses fully qualified names (`table.column`) to split into table/column

4. **OutputWriter:**
   - Writes to file if `output_file` specified
   - Otherwise prints to stdout

**Design Notes:**
- All formatters are static methods for simplicity
- Separate `format()` and `format_table()` methods for column vs table lineage
- Use Pydantic's `model_dump()` for JSON serialization

### 4. File Utilities (`utils/file_utils.py`)

**Purpose:** File I/O operations with proper error handling

```python
def read_sql_file(file_path: Path) -> str
```

**Error Handling:**
- FileNotFoundError: File doesn't exist
- ValueError: Path is not a file
- PermissionError: Cannot read file
- UnicodeDecodeError: File not UTF-8 encoded

### 5. Configuration System (`utils/config.py`)

**Purpose:** Load and manage configuration from `sqlglider.toml`

**Data Model (Pydantic):**

```python
class ConfigSettings(BaseModel):
    dialect: Optional[str] = None
    level: Optional[str] = None
    output_format: Optional[str] = None
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
```

**Design Notes:**
- Uses Python's built-in `tomllib` (Python 3.11+, zero external dependencies)
- Config is project-specific (PWD only, no user-level config)
- Fail-safe: Never crashes on config errors
- Forward compatible: Ignores unknown settings for future features

## Technology Stack

### Core Dependencies

- **sqlglot[rs] >= 25.0.0:** SQL parser and lineage analysis library with Rust extensions
  - Provides SQL parsing, AST manipulation, and lineage analysis
  - The `[rs]` extra includes Rust-accelerated components for better performance

- **typer >= 0.9.0:** CLI framework with type hints and automatic help generation

- **rich >= 13.0.0:** Terminal formatting and colored output

- **pydantic >= 2.0.0:** Data validation and serialization with type hints

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

**Decision:** Implement reverse lineage by running forward lineage and inverting the dependency graph, reusing the `ColumnLineage` model with semantic field swap

**Rationale:**
- **Leverages existing code:** Reuses robust `analyze_column_lineage()` implementation
- **Handles edge cases:** Automatically inherits all SQLGlot edge case handling (CTEs, subqueries, complex expressions)
- **Simple algorithm:** O(n) time complexity with straightforward graph inversion
- **Zero formatter changes:** Semantic field reuse means all formatters (text, JSON, CSV) work without modification
- **Maintainability:** Less code to maintain, single data model, consistent API
- **Performance:** Negligible overhead even for complex queries (typically <100ms)

**Alternative Considered:**
- Create separate `ReverseColumnLineage` model → Rejected due to code duplication and formatter changes required
- Traverse SQLGlot Node tree from root → Rejected because Node doesn't expose parent/upstream references
- Build bidirectional graph → Rejected as over-engineering for current needs

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

### Column-Level Forward Lineage

1. **Parse SQL:** Create AST using `sqlglot.parse_one()`
2. **Extract Output Columns:** Traverse SELECT expressions
3. **For Each Output Column:**
   - Call `sqlglot.lineage.lineage(column, sql, dialect)`
   - Receive a `Node` tree with `downstream` references
   - Recursively traverse tree depth-first
   - Collect leaf nodes (no downstream) as source columns
   - Sort and deduplicate sources
4. **Return Results:** List of `ColumnLineage` objects

### Column-Level Reverse Lineage (Impact Analysis)

1. **Run Forward Lineage:** Call `analyze_column_lineage()` for all output columns
2. **Build Reverse Map:** Invert the dependency graph
   - For each forward lineage result (output → sources)
   - Create reverse mapping (source → [outputs])
   - Example: If `customer_id` comes from `orders.customer_id`, map `orders.customer_id` → `customer_id`
3. **Validate Source Column:** Check that requested source exists in reverse map
   - If not found, raise `ValueError` with list of available sources
4. **Return Results:** List containing one `ColumnLineage` object with semantic field swap:
   - `output_column` = the source column being analyzed
   - `source_columns` = list of affected output columns (sorted)

**Design Note - Semantic Field Reuse:**
The reverse lineage reuses the `ColumnLineage` model with inverted semantics. This clever design:
- Eliminates need for a separate data model
- Allows all formatters (text, JSON, CSV) to work without modification
- Maintains API consistency between forward and reverse modes
- Simplifies the codebase while providing full functionality

**Algorithm Complexity:**
- Time: O(n + n*m) where n = output columns, m = avg sources per column → O(n*m) typically O(n)
- Space: O(n*m) for the reverse mapping dictionary
- Performance: Negligible overhead (<100ms even for complex queries with 50+ columns)

### Table-Level Lineage

1. **Parse SQL:** Create AST using `sqlglot.parse_one()`
2. **Find All Tables:** Search for `exp.Table` nodes in AST
3. **Collect Table Names:** Get fully qualified table names
4. **Return Results:** `TableLineage` object with sources

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
