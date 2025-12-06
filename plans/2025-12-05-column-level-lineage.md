# Column-Level Lineage Implementation Plan

**Date:** 2024-12-05
**Status:** Completed
**Feature:** Column and Table Level Lineage Analysis CLI

## Overview

Implement a CLI tool using Typer and SQLGlot to analyze column-level and table-level lineage for SparkSQL queries. The tool will parse standalone SQL files and trace dependencies through multiple levels (CTEs, subqueries, etc.).

## Requirements

### Command Structure
```bash
sqlglider lineage $SQLFILE_NAME --level column
```

### CLI Options
- `--level`: Choose between "column" (default) or "table" lineage
- `--dialect`: SQL dialect, default to "spark" but allow others (postgres, snowflake, etc.)
- `--column`: Optional - specify a single column name to analyze (default: all columns)
- `--output-format`: Choose between "text" (default), "json", or "csv"
- `--output-file`: Optional - export results to a file instead of stdout

### Input
- Standalone SQL file (not a full SQLMesh project)
- Should parse and analyze the SQL query directly

### Output Format (text mode)
```
----------
tableAdatabaseName.tableATableName.columnA
----------
database.tableAInputTable.columnC
database.tableAInputTable.columnF
database.tableAInputTable2.columnQ
----------
tableAdatabaseName.tableATableName.columnB
----------
database.tableAInputTable.columnC
database.tableAInputTable.columnF
database.tableAInputTable3.columnZ
```

### Lineage Behavior
- Trace through multiple levels of dependencies (recursive)
- For column level: show which input columns contribute to each output column
- For table level: show which input tables contribute to output tables

## Design Decisions

### 1. Use SQLGlot Directly Instead of SQLMesh
**Rationale:**
- SQLMesh is designed for full project environments with models and config
- SQL Glider targets standalone SQL files for ad-hoc analysis
- SQLGlot provides the underlying lineage API we need
- Simpler, more lightweight implementation

### 2. Pydantic for Data Models
**Rationale:**
- Built-in JSON serialization via `model_dump()`
- Runtime type validation
- Better IDE support and documentation
- Extensibility for future features

### 3. CLI Structure with @app.callback()
**Rationale:**
- User requirement: `sqlglider lineage <file>`
- Typer makes single commands the default (would be just `sqlglider <file>`)
- Callback prevents default behavior, making `lineage` explicit
- Allows for future commands to be added easily

### 4. Three Output Formats
**Rationale:**
- Text: Human-readable, matches user's specified format
- JSON: Machine-readable, structured data for downstream tools
- CSV: Tabular format for spreadsheet analysis

## Implementation Steps

### 1. Update Project Configuration
- [x] Add dependencies to `pyproject.toml`:
  - `typer>=0.9.0` - CLI framework
  - `rich>=13.0.0` - Terminal formatting
  - `pydantic>=2.0.0` - Data models
- [x] Add CLI entry point: `sqlglider = "sqlglider.cli:app"`
- [x] Enable packaging: `tool.uv.package = true`

### 2. Create Package Structure
- [x] Create `src/sqlglider/` package directory
- [x] Create `src/sqlglider/lineage/` module
- [x] Create `src/sqlglider/utils/` module
- [x] Create all `__init__.py` files with exports

### 3. Implement File Utilities
- [x] Create `src/sqlglider/utils/file_utils.py`
- [x] Implement `read_sql_file()` function
- [x] Handle errors: FileNotFoundError, PermissionError, UnicodeDecodeError

### 4. Implement Core Lineage Analyzer
- [x] Create `src/sqlglider/lineage/analyzer.py`
- [x] Define Pydantic models:
  - `ColumnLineage(output_column, source_columns)`
  - `TableLineage(output_table, source_tables)`
- [x] Implement `LineageAnalyzer` class:
  - `__init__(sql, dialect)` - Parse SQL with SQLGlot
  - `get_output_columns()` - Extract output columns from SELECT
  - `analyze_column_lineage(column)` - Analyze column dependencies
  - `analyze_table_lineage()` - Analyze table dependencies
  - `_collect_source_columns(node, sources)` - Recursive tree traversal

**Key Algorithm:**
```python
def _collect_source_columns(self, node: Node, sources: Set[str]) -> None:
    if not node.downstream:
        # Leaf node - this is a source column
        sources.add(node.name)
    else:
        # Traverse deeper
        for child in node.downstream:
            self._collect_source_columns(child, sources)
```

### 5. Implement Output Formatters
- [x] Create `src/sqlglider/lineage/formatters.py`
- [x] Implement `TextFormatter` - matches specified format
- [x] Implement `JsonFormatter` - structured JSON with Pydantic
- [x] Implement `CsvFormatter` - parse qualified names for table/column split
- [x] Implement `OutputWriter` - write to file or stdout

### 6. Implement CLI Entry Point
- [x] Create `src/sqlglider/cli.py`
- [x] Define Typer app with callback to force explicit `lineage` command
- [x] Implement `lineage()` command with all options
- [x] Add validation for level, output format
- [x] Comprehensive error handling:
  - File not found errors
  - SQL parsing errors
  - Column not found errors
  - Invalid parameters
- [x] Use Rich console for colored output with stderr support

### 7. Update Main Entry Point
- [x] Update `main.py` to call CLI for backward compatibility

### 8. Testing
- [x] Install dependencies: `uv sync`
- [x] Test basic column lineage
- [x] Test JSON output format
- [x] Test CSV output format
- [x] Test specific column filtering
- [x] Test table-level lineage
- [x] Test file output
- [x] Test multi-level lineage with CTEs

## Files Created/Modified

### Created Files
1. `src/sqlglider/__init__.py` - Package initialization
2. `src/sqlglider/cli.py` - Typer CLI entry point
3. `src/sqlglider/lineage/__init__.py` - Lineage module exports
4. `src/sqlglider/lineage/analyzer.py` - Core lineage logic
5. `src/sqlglider/lineage/formatters.py` - Output formatters
6. `src/sqlglider/utils/__init__.py` - Utils module exports
7. `src/sqlglider/utils/file_utils.py` - File utilities
8. `tests/__init__.py` - Test package
9. `ARCHITECTURE.md` - Technical documentation

### Modified Files
1. `pyproject.toml` - Added dependencies and CLI entry point
2. `main.py` - Updated to call CLI
3. `CLAUDE.md` - Updated with new structure and guidelines

## Testing Strategy

### Manual Testing Performed
- [x] Basic column lineage: `uv run sqlglider lineage test_query.sql`
- [x] JSON output: `uv run sqlglider lineage test_query.sql --output-format json`
- [x] CSV output: `uv run sqlglider lineage test_query.sql --output-format csv`
- [x] Column filtering: `uv run sqlglider lineage test_query.sql --column customer_name`
- [x] Table lineage: `uv run sqlglider lineage test_query.sql --level table`
- [x] File export: `uv run sqlglider lineage test_query.sql --output-file lineage.json`
- [x] Multi-level CTE lineage: `uv run sqlglider lineage test_cte_query.sql`

### Test Fixtures Created
1. `test_query.sql` - Simple JOIN query
2. `test_cte_query.sql` - Complex query with CTEs

### Future Testing (To Be Implemented)
- Unit tests for `LineageAnalyzer`
- Unit tests for formatters
- Integration tests for CLI
- Error handling tests
- Edge case tests (wildcard selects, complex expressions)

## Technical Challenges Resolved

### 1. Typer Default Command Behavior
**Problem:** Typer makes single commands the default, preventing `sqlglider lineage` syntax

**Solution:** Added `@app.callback()` to force explicit command structure

### 2. Unicode Characters in Terminal
**Problem:** Checkmark character (✓) failed on Windows terminal

**Solution:** Replaced with "Success:" text label

### 3. Rich Console stderr Parameter
**Problem:** `console.print(file=sys.stderr)` not supported

**Solution:** Use `console.print(stderr=True)` instead

### 4. Extracting Output Columns
**Problem:** Need to identify all output columns including aliases and expressions

**Solution:** Traverse `select_node.expressions` and handle `exp.Alias`, `exp.Column`, and fallback to SQL representation

### 5. Multi-Level Lineage Tracing
**Problem:** Need to trace through CTEs and subqueries to find ultimate sources

**Solution:** Recursive tree traversal of SQLGlot's `Node.downstream` structure, collecting leaf nodes

## Success Criteria

- ✅ CLI accepts SQL file and analyzes lineage
- ✅ Supports both column and table level analysis
- ✅ Traces through multiple dependency levels
- ✅ Output matches specified text format
- ✅ Supports JSON and CSV export
- ✅ Handles SparkSQL dialect (and others via --dialect)
- ✅ Can filter to specific column
- ✅ Clear error messages for invalid input
- ⏳ Comprehensive test coverage (future work)

## Future Enhancements

### Potential Features
1. Schema support: `--schema` option for better lineage accuracy
2. Reverse lineage: Impact analysis from source columns
3. Visualization: Generate HTML lineage graphs
4. Batch processing: Analyze multiple SQL files
5. Configuration file: `.sqlglider.toml` for defaults
6. Query validation: Validate against schemas

### Performance Optimizations
- Cache parsed ASTs for repeated analysis
- Parallel processing for batch operations
- Lazy evaluation of lineage (only requested columns)

## Implementation Notes

### Deviations from Original Plan
- **Pydantic Models:** Implemented using Pydantic instead of dataclasses (user preference)
  - Provides better JSON serialization via `model_dump()`
  - Adds runtime validation and better IDE support
- **No SQLMesh Context:** Confirmed decision to use SQLGlot directly without SQLMesh's Context API
  - Simpler implementation for standalone SQL files
  - SQLGlot provides all needed lineage functionality

### Technical Challenges Encountered

1. **Typer Default Command Behavior**
   - **Challenge:** Typer automatically makes single commands the default, preventing explicit `lineage` subcommand
   - **Solution:** Added `@app.callback()` to force explicit command structure
   - **Result:** Users can now use `sqlglider lineage <file>` as intended

2. **Unicode Characters in Windows Terminal**
   - **Challenge:** Checkmark character (✓) caused encoding errors on Windows
   - **Solution:** Replaced with text label "Success:" instead
   - **Result:** Cross-platform compatibility maintained

3. **Rich Console stderr Support**
   - **Challenge:** `console.print(file=sys.stderr)` not supported by Rich
   - **Solution:** Use `console.print(stderr=True)` parameter instead
   - **Result:** Proper error output to stderr with Rich formatting

4. **Multi-Level Lineage Tracing**
   - **Challenge:** Needed to trace through CTEs and subqueries to ultimate sources
   - **Solution:** Recursive depth-first traversal of SQLGlot's Node tree
   - **Result:** Automatically handles arbitrary nesting levels

### Additional Features
- Added comprehensive error handling with user-friendly messages
- Included Rich console formatting for better UX
- Created extensive documentation (ARCHITECTURE.md, updated CLAUDE.md)
- Established plan documentation workflow

### Known Limitations
- **No schema support:** Cannot validate column existence or handle `SELECT *` expansion
- **No visualization:** Text/JSON/CSV only, no graphical lineage graphs
- **Single file processing:** No batch mode for analyzing multiple files
- **No caching:** Re-parses SQL on each run

## Lessons Learned

### What Worked Well

1. **SQLGlot's Lineage API:** Extremely powerful and handles complex SQL patterns
   - CTEs, subqueries, joins all work seamlessly
   - Multi-level tracing is automatic with recursive traversal
   - Dialect support covers most SQL variants

2. **Pydantic Models:** Excellent choice for data structures
   - JSON serialization is trivial with `model_dump()`
   - Type validation prevents bugs
   - Self-documenting with Field descriptions

3. **Modular Architecture:** Clean separation of concerns
   - CLI, analysis, and formatting are independent
   - Easy to test individual components
   - Simple to extend with new features

4. **Typer CLI Framework:** Intuitive and powerful
   - Automatic help generation from docstrings
   - Type hints provide validation
   - Rich integration for beautiful output

### What Could Be Improved

1. **Testing:** No unit tests implemented yet
   - Should add comprehensive test suite
   - Integration tests for end-to-end CLI validation
   - Property-based testing for edge cases

2. **Schema Support:** Would significantly improve accuracy
   - Could resolve `SELECT *` to actual columns
   - Could validate query correctness
   - Could provide better error messages

3. **Performance:** Not optimized for large-scale use
   - Could cache parsed ASTs
   - Could add parallel processing for batch mode
   - Could implement incremental analysis

### Insights for Future Features

1. **Start with Pydantic:** Use Pydantic for all data models from the beginning
   - Better serialization, validation, and documentation
   - Well worth the dependency

2. **Plan for Extensibility:** Modular architecture pays off
   - New output formats can be added easily
   - New analysis types fit naturally
   - Independent components are easier to test

3. **Error Handling Matters:** User experience is critical
   - Clear error messages prevent frustration
   - Validation catches issues early
   - Proper stderr output helps with debugging

4. **Documentation is Investment:** Comprehensive docs save time
   - ARCHITECTURE.md clarifies design decisions
   - Plan documents capture context
   - Future developers (including AI) benefit greatly

## Conclusion

Successfully implemented a complete column-level lineage analysis CLI tool that:
- Uses SQLGlot for powerful SQL parsing and lineage analysis
- Provides flexible output formats (text, JSON, CSV)
- Supports multiple SQL dialects
- Handles multi-level dependency tracing
- Offers intuitive CLI with comprehensive error handling
- Maintains clean, modular architecture for future extensions

The tool is production-ready for analyzing standalone SQL files and provides the foundation for future enhancements like schema validation, visualization, and batch processing.

**Key Success Factors:**
- Leveraging SQLGlot's mature lineage API
- Choosing Pydantic for robust data modeling
- Maintaining clear separation of concerns
- Comprehensive documentation and planning

**Next Steps:**
- Add unit and integration tests
- Implement schema support for improved accuracy
- Consider visualization capabilities
- Explore batch processing for multiple files
