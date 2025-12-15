# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQL Glider is a SQL Utility Toolkit for better understanding, use, and governance of SQL queries in a native environment. The project provides column-level and table-level lineage analysis for standalone SQL files using SQLGlot's powerful parsing and lineage capabilities.

**IMPORTANT:** See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical documentation about the project structure, design decisions, and implementation details.

## Development Setup

This project uses `uv` for Python package management. Python 3.11+ is required.

### Package Management
- Install dependencies: `uv sync`
- Add new dependencies: `uv add <package-name>`
- Add dev dependencies: `uv add --dev <package-name>`

### Running the Application
- Run CLI: `uv run sqlglider lineage <sql_file>`
- Run main script (backward compatibility): `uv run python main.py`

### Code Quality
- Linting and formatting: `uv run ruff check`
- Auto-fix issues: `uv run ruff check --fix`
- Format code: `uv run ruff format`
- Type checking: `uv run basedpyright`

### Testing
- Run all tests: `uv run pytest`
- Run with coverage: `uv run pytest --cov=sqlglider --cov-report=term-missing`
- Run with coverage threshold check: `uv run pytest --cov=sqlglider --cov-fail-under=80`
- Run specific test file: `uv run pytest tests/test_case_insensitive.py`
- Run tests matching pattern: `uv run pytest -k "case_insensitive"`
- Verbose output: `uv run pytest -v`
- Generate HTML coverage report: `uv run pytest --cov=sqlglider --cov-report=html`

**Coverage Requirements:**
- Minimum coverage threshold: **80%** (with branch coverage)
- This threshold is automatically enforced via `pyproject.toml` configuration
- Tests will fail if coverage drops below 80%

## Key Dependencies

- **sqlglot[rs]**: SQL parser and lineage analysis library with Rust extensions
- **typer**: CLI framework with type hints
- **rich**: Terminal formatting and colored output
- **pydantic**: Data validation and serialization
- **rustworkx**: High-performance graph library for cross-file lineage
- **jinja2**: Template engine for SQL preprocessing
- **ruff**: Fast Python linter and formatter (dev dependency)
- **basedpyright**: Static type checker (dev dependency)
- **pytest**: Testing framework (dev dependency)
- **pytest-cov**: Coverage plugin for pytest (dev dependency)

## Project Structure

```
src/sqlglider/
├── cli.py                    # Typer CLI entry point
├── dissection/
│   ├── analyzer.py           # DissectionAnalyzer for query decomposition
│   ├── formatters.py         # Output formatters (text, JSON, CSV)
│   └── models.py             # ComponentType, SQLComponent, QueryDissectionResult
├── graph/
│   ├── builder.py            # Build lineage graphs from SQL files
│   ├── merge.py              # Merge multiple graphs
│   ├── query.py              # Query upstream/downstream lineage
│   ├── models.py             # Graph data models (Pydantic)
│   └── serialization.py      # JSON save/load for graphs
├── lineage/
│   ├── analyzer.py           # Core lineage analysis using SQLGlot
│   └── formatters.py         # Output formatters (text, JSON, CSV)
├── templating/
│   ├── base.py               # Templater base class and NoOpTemplater
│   ├── jinja.py              # Jinja2 templater implementation
│   ├── registry.py           # Plugin discovery via entry points
│   └── variables.py          # Variable loading from multiple sources
└── utils/
    ├── config.py             # Configuration file loading
    └── file_utils.py         # File I/O utilities
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for complete project structure and component details.

## CLI Usage

### Single Query Files

```bash
# Forward lineage: Find sources for all output columns
uv run sqlglider lineage query.sql

# Forward lineage: Specific output column
uv run sqlglider lineage query.sql --column customer_name

# Reverse lineage: Find outputs affected by a source column (impact analysis)
uv run sqlglider lineage query.sql --source-column orders.customer_id

# JSON output
uv run sqlglider lineage query.sql --output-format json

# Export to file
uv run sqlglider lineage query.sql --output-format csv --output-file lineage.csv

# Table-level lineage
uv run sqlglider lineage query.sql --level table

# Different SQL dialect
uv run sqlglider lineage query.sql --dialect postgres
```

### Reading from Stdin

All commands (`lineage`, `tables overview`, `tables pull`, `template`) support reading SQL from stdin when no file is provided:

```bash
# Pipe SQL directly to lineage analysis
echo "SELECT id, name FROM users" | uv run sqlglider lineage

# Pipe SQL with options
echo "SELECT * FROM orders" | uv run sqlglider lineage --output-format json

# Use heredoc for multi-line SQL
uv run sqlglider lineage << 'EOF'
SELECT
    c.customer_id,
    c.name,
    o.total
FROM customers c
JOIN orders o ON c.id = o.customer_id
EOF

# Pipe to tables overview command
echo "SELECT * FROM users JOIN orders ON users.id = orders.user_id" | uv run sqlglider tables overview

# Pipe template with variables
echo "SELECT * FROM {{ schema }}.users" | uv run sqlglider template --var schema=prod

# Chain commands: generate SQL and analyze
cat query.sql | uv run sqlglider template --var env=prod | uv run sqlglider lineage
```

### Multi-Query Files

SQL Glider supports files with multiple SQL statements separated by semicolons:

```bash
# Analyze all queries in a file (automatic multi-query detection)
uv run sqlglider lineage multi_query.sql

# Filter to only queries that reference a specific table
uv run sqlglider lineage multi_query.sql --table customers

# Analyze specific column across all queries
uv run sqlglider lineage multi_query.sql --column customers.customer_id

# Export multi-query results to JSON
uv run sqlglider lineage multi_query.sql --output-format json

# Export multi-query results to CSV (includes query_index column)
uv run sqlglider lineage multi_query.sql --output-format csv

# Reverse lineage across all queries (impact analysis)
uv run sqlglider lineage multi_query.sql --source-column customers.customer_id

# Table-level lineage for all queries
uv run sqlglider lineage multi_query.sql --level table

# Combine table filter with other options
uv run sqlglider lineage multi_query.sql --table orders --source-column orders.customer_id
```

### Table Extraction

Extract all tables involved in SQL files with usage and type information:

```bash
# List all tables in a SQL file
uv run sqlglider tables overview query.sql

# JSON output
uv run sqlglider tables overview query.sql --output-format json

# CSV output
uv run sqlglider tables overview query.sql --output-format csv

# Export to file
uv run sqlglider tables overview query.sql --output-format csv --output-file tables.csv

# Different SQL dialect
uv run sqlglider tables overview query.sql --dialect postgres

# Filter to queries referencing a specific table (multi-query files)
uv run sqlglider tables overview multi_query.sql --table customers

# With templating support
uv run sqlglider tables overview query.sql --templater jinja --var schema=analytics
```

**Output includes:**
- **Table Name**: Fully qualified table name (e.g., `schema.table`)
- **Usage**: `INPUT` (read from), `OUTPUT` (written to), or `BOTH`
- **Object Type**: `TABLE`, `VIEW`, `CTE`, or `UNKNOWN`

### DDL Retrieval from Remote Catalogs

Pull DDL definitions from remote data catalogs for tables used in SQL:

```bash
# Pull DDL for tables in a SQL file (output to stdout)
uv run sqlglider tables pull query.sql --catalog-type databricks

# Pull DDL to a folder (one file per table)
uv run sqlglider tables pull query.sql -c databricks -o ./ddl/

# With templating
uv run sqlglider tables pull query.sql -c databricks --templater jinja --var schema=prod

# From stdin
echo "SELECT * FROM my_catalog.my_schema.users" | uv run sqlglider tables pull -c databricks

# List available catalog providers
uv run sqlglider tables pull --list
```

**Notes:**
- Requires optional dependency: `pip install sql-glider[databricks]`
- CTEs are automatically excluded (they don't exist in remote catalogs)
- Configure authentication via environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`) or `sqlglider.toml`

### Graph-Based Lineage (Cross-File Analysis)

```bash
# Build graph from single file
uv run sqlglider graph build query.sql -o graph.json

# Build from multiple files
uv run sqlglider graph build query1.sql query2.sql -o graph.json

# Build from directory (recursive)
uv run sqlglider graph build ./queries/ -r -o graph.json

# Build with custom glob pattern
uv run sqlglider graph build ./queries/ -g "*.spark.sql" -o graph.json

# Build from manifest CSV
uv run sqlglider graph build --manifest manifest.csv -o graph.json

# Build with specific dialect and node format
uv run sqlglider graph build ./queries/ -o graph.json --dialect postgres --node-format structured

# Merge multiple graphs
uv run sqlglider graph merge graph1.json graph2.json -o merged.json

# Merge with glob pattern
uv run sqlglider graph merge --glob "graphs/*.json" -o merged.json

# Query upstream dependencies
uv run sqlglider graph query graph.json --upstream orders.customer_id

# Query downstream dependencies
uv run sqlglider graph query graph.json --downstream customers.id

# Query with JSON output
uv run sqlglider graph query graph.json --upstream orders.total -f json

# Query with CSV output
uv run sqlglider graph query graph.json --downstream customers.id -f csv
```

### SQL Templating

SQL Glider supports Jinja2 templating for SQL files. This allows you to use variables, conditionals, and loops in your SQL before analysis.

```bash
# Render a SQL template with variables
uv run sqlglider template query.sql --var schema=analytics --var table=users

# Use a variables file (JSON or YAML)
uv run sqlglider template query.sql --vars-file vars.json

# Output rendered SQL to file
uv run sqlglider template query.sql --var schema=prod -o rendered.sql

# List available templaters
uv run sqlglider template query.sql --list

# Use the no-op templater (pass through unchanged)
uv run sqlglider template query.sql --templater none

# Lineage analysis with templating
uv run sqlglider lineage query.sql --templater jinja --var schema=analytics

# Graph build with templating
uv run sqlglider graph build ./queries/ -o graph.json --templater jinja --var schema=prod
```

**Template Syntax (Jinja2):**
```sql
-- Variables
SELECT * FROM {{ schema }}.{{ table }}

-- Conditionals
SELECT
    customer_id
    {% if include_total %}, SUM(amount) as total{% endif %}
FROM orders

-- Loops
SELECT {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
FROM users

-- Includes
{% include 'common_cte.sql' %}
SELECT * FROM cte
```

**Variable Priority (highest to lowest):**
1. CLI arguments (`--var key=value`)
2. Variables file (`--vars-file vars.json`)
3. Config file (`[sqlglider.templating.variables]`)
4. Environment variables (`SQLGLIDER_VAR_*`)

### Query Dissection

Decompose SQL queries into constituent parts for unit testing and analysis:

```bash
# Dissect a SQL file (text output)
uv run sqlglider dissect query.sql

# JSON output with full component details
uv run sqlglider dissect query.sql --output-format json

# CSV output for spreadsheet analysis
uv run sqlglider dissect query.sql --output-format csv

# Export to file
uv run sqlglider dissect query.sql --output-format json --output-file dissected.json

# With templating support
uv run sqlglider dissect query.sql --templater jinja --var schema=analytics

# From stdin
echo "WITH cte AS (SELECT id FROM users) SELECT * FROM cte" | uv run sqlglider dissect
```

**Extracted Component Types:**
- `CTE`: Common Table Expressions from WITH clause
- `MAIN_QUERY`: The primary SELECT statement
- `SUBQUERY`: Nested SELECT in FROM clause
- `SCALAR_SUBQUERY`: Single-value subquery in SELECT list, WHERE, HAVING
- `TARGET_TABLE`: Output table for INSERT/CREATE/MERGE (not executable)
- `SOURCE_QUERY`: SELECT within DML/DDL statements
- `UNION_BRANCH`: Individual SELECT in UNION/UNION ALL

**Use Cases:**
- Unit test CTEs and subqueries individually
- Extract DQL from CTAS, CREATE VIEW, INSERT statements
- Analyze query structure and component dependencies
- Break apart complex queries for understanding

## Development Guidelines

### Code Style
- Use Pydantic models for data structures (not dataclasses)
- Type hints required for all functions
- Docstrings for all public APIs
- Follow PEP 8 conventions
- Keep functions focused and small

### Configuration Files

**User Configuration:**
- `sqlglider.toml` in current working directory provides default CLI options
- Config is optional - CLI works without it
- Priority: CLI args > config file > hardcoded defaults
- Config is project-specific (PWD only, no user-level config)

**Modifying Configurable Options:**
When adding new configurable CLI options:
1. Add field to `ConfigSettings` in `src/sqlglider/utils/config.py`
2. Update CLI option default to `None` in `src/sqlglider/cli.py`
3. Add priority resolution logic: `value = cli_arg or config.field or default`
4. Update tests in `tests/sqlglider/utils/test_config.py`
5. Update `sqlglider.toml.example` with new option
6. Document in README.md and ARCHITECTURE.md

**Config File Format:**
```toml
[sqlglider]
dialect = "postgres"
level = "column"
output_format = "json"
```

**Supported Options:**
- `dialect` - SQL dialect (e.g., "postgres", "snowflake", "spark")
- `level` - Analysis level ("column" or "table")
- `output_format` - Output format ("text", "json", or "csv")
- `templater` - Templater for SQL preprocessing ("jinja", "none", or custom)
- `[sqlglider.templating]` section for template variables and variables file

### When Making Changes
1. **Update ARCHITECTURE.md** if you change:
   - Project structure (new modules, files, or directories)
   - Core components or their responsibilities
   - Key design decisions or algorithms
   - Technology stack or dependencies
   - CLI interface or command structure
2. Update this file (CLAUDE.md) if you change development workflows
3. Update README.md if you change user-facing features
4. Run linter and type checker before committing:
   ```bash
   uv run ruff check --fix && uv run ruff format
   uv run basedpyright
   ```

### Adding New Features
- **Add unit tests for new functionality** - This is MANDATORY
- Unit tests must pass before a feature is considered complete
- Update all relevant documentation
- Maintain backward compatibility where possible
- Consider error handling and user experience

## Testing Guidelines

### Testing Framework: PyTest

SQL Glider uses **pytest** as the primary testing framework. All unit tests must be written using pytest conventions.

### Testing Requirements

**MANDATORY:**
- All new features MUST have corresponding unit tests
- All bug fixes MUST have regression tests
- Unit tests MUST pass before a feature is considered complete
- Tests should be added/updated in the same commit as the feature implementation

### Test Directory Structure

**IMPORTANT:** Test files must mirror the source code directory structure.

```
tests/
├── __init__.py
├── sqlglider/
│   ├── __init__.py
│   ├── graph/
│   │   ├── __init__.py
│   │   ├── test_builder.py           # Tests for graph builder
│   │   ├── test_merge.py             # Tests for graph merger
│   │   ├── test_models.py            # Tests for graph models
│   │   ├── test_query.py             # Tests for graph querier
│   │   └── test_serialization.py     # Tests for serialization
│   ├── lineage/
│   │   ├── __init__.py
│   │   ├── test_analyzer.py          # Tests for analyzer.py
│   │   └── test_formatters.py        # Tests for formatters.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── test_file_utils.py        # Tests for file_utils.py
│   └── test_cli.py                   # Tests for cli.py
└── fixtures/
    ├── sample_queries.sql            # Shared test SQL files
    ├── sample_manifest.csv           # Example manifest file
    └── multi_file_queries/           # Multi-file test fixtures
        ├── customers.sql
        ├── orders.sql
        └── reports.sql
```

**Naming Convention:**
- Source: `src/sqlglider/lineage/analyzer.py`
- Test: `tests/sqlglider/lineage/test_analyzer.py`

The test file name is the source file name prefixed with `test_`.

### PyTest Best Practices

1. **Use Parameterized Tests** for testing multiple inputs/outputs:
   ```python
   @pytest.mark.parametrize(
       "input_value,expected_output",
       [
           ("lowercase", "result1"),
           ("UPPERCASE", "result2"),
           ("MiXeD", "result3"),
       ],
   )
   def test_function(input_value, expected_output):
       assert process(input_value) == expected_output
   ```

2. **Use Fixtures** for shared test data and setup:
   ```python
   @pytest.fixture
   def sample_query():
       return "SELECT * FROM table"

   def test_something(sample_query):
       analyzer = LineageAnalyzer(sample_query)
       # test code
   ```

3. **Organize Tests by Class** for related test groups:
   ```python
   class TestCaseInsensitiveMatching:
       """Tests for case-insensitive column matching."""

       def test_lowercase(self):
           # test code

       def test_uppercase(self):
           # test code
   ```

4. **Use Descriptive Test Names** that explain what is being tested:
   - Good: `test_column_matching_is_case_insensitive`
   - Bad: `test_columns`

5. **Test Both Success and Failure Cases**:
   ```python
   def test_valid_column_returns_lineage(self):
       # test successful case

   def test_invalid_column_raises_value_error(self):
       with pytest.raises(ValueError):
           # test error case
   ```

6. **Use pytest-cov for Coverage Analysis**:
   - Aim for >80% code coverage for core modules
   - Use `uv run pytest --cov=sqlglider --cov-report=term-missing` to identify gaps

### PyTest Helper Libraries

Feel free to use pytest ecosystem libraries when they provide value:

- **pytest-cov**: Code coverage reporting (already included)
- **pytest-mock**: Mocking and patching utilities
- **pytest-timeout**: Timeout for long-running tests
- **pytest-xdist**: Parallel test execution
- **pytest-benchmark**: Performance benchmarking

Install additional pytest plugins as needed:
```bash
uv add --dev pytest-mock pytest-timeout
```

### Testing Patterns for SQL Glider

#### Testing Lineage Analysis

```python
def test_lineage_analysis():
    sql = "SELECT customer_id FROM orders"
    analyzer = LineageAnalyzer(sql, dialect="spark")
    results = analyzer.analyze_queries(level="column")

    assert len(results) == 1  # One query
    assert results[0].metadata.query_index == 0
    assert len(results[0].lineage_items) == 1  # One column
    assert results[0].lineage_items[0].output_name == "orders.customer_id"
    assert results[0].lineage_items[0].source_name == "orders.customer_id"
```

#### Testing CLI Commands (Future)

```python
from typer.testing import CliRunner

def test_cli_command():
    runner = CliRunner()
    result = runner.invoke(app, ["lineage", "test_query.sql"])

    assert result.exit_code == 0
    assert "customer_id" in result.stdout
```

#### Testing Error Handling

```python
def test_invalid_sql_raises_parse_error():
    sql = "INVALID SQL SYNTAX"

    with pytest.raises(ParseError):
        analyzer = LineageAnalyzer(sql, dialect="spark")
        analyzer.analyze_queries(level="column")
```

### Running Tests During Development

**Before committing changes:**
```bash
# Run all tests
uv run pytest

# Run with coverage to ensure adequate test coverage
uv run pytest --cov=sqlglider --cov-report=term-missing

# Run linter
uv run ruff check --fix && uv run ruff format
```

**During active development:**
```bash
# Run tests for specific file you're working on
uv run pytest tests/sqlglider/lineage/test_analyzer.py -v

# Run tests matching a pattern
uv run pytest -k "case_insensitive" -v

# Stop on first failure for faster debugging
uv run pytest -x

# Show print statements (useful for debugging)
uv run pytest -s
```

### Continuous Integration (Future)

When CI is set up:
- Tests must pass on all supported Python versions (3.11+)
- Code coverage must meet minimum threshold (80%+)
- Linting must pass without errors
- All checks must pass before merging PRs

### Planning and Design Documents

**During Planning Phase:**
When Claude Code completes a planning phase for a new feature or significant change:
1. Create a `plans/` directory in the project root if it doesn't exist
2. Save the final plan as a Markdown file in `plans/` with a descriptive name
   - Format: `plans/YYYY-MM-DD-feature-name.md`
   - Example: `plans/2025-12-05-column-level-lineage.md`
3. Include in the plan:
   - Overview of the feature or change
   - Design decisions and rationale
   - Implementation steps (checkboxes: `- [ ]` for pending)
   - Files to be created or modified
   - Testing strategy
   - Status field at top: `**Status:** Planned`

**After Implementation:**
When implementation of a planned feature is complete:
1. Update the corresponding plan file in `plans/`
2. Change status from `Planned` to `Completed` or `Partially Completed`
3. Mark completed implementation steps with checkboxes: `- [x]`
4. Add an "Implementation Notes" section documenting:
   - Any deviations from the original plan
   - Technical challenges encountered and how they were resolved
   - Additional features or changes made during implementation
   - Known limitations or future improvements needed
5. Update the "Testing Strategy" section with:
   - Tests that were actually performed
   - Test results and any issues found
   - Coverage gaps or future testing needs
6. Add a "Lessons Learned" section (optional but recommended):
   - What worked well in the design
   - What could be improved
   - Insights for future similar features

Keep plans in the repository as living documents that serve as:
- Historical record of feature development
- Reference for understanding implementation choices
- Context for future maintainers and AI assistants
- Template and learning resource for similar features

## References

- Ruff documentation: https://docs.astral.sh/ruff/configuration/
- UV documentation: https://docs.astral.sh/uv/
- Typer documentation: https://typer.tiangolo.com/
- SQLGlot documentation: https://sqlglot.com/
- Pydantic documentation: https://docs.pydantic.dev/
- rustworkx documentation: https://www.rustworkx.org/
