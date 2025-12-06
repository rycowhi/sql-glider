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
- **ruff**: Fast Python linter and formatter (dev dependency)
- **pytest**: Testing framework (dev dependency)
- **pytest-cov**: Coverage plugin for pytest (dev dependency)

## Project Structure

```
src/sqlglider/
├── cli.py                    # Typer CLI entry point
├── lineage/
│   ├── analyzer.py           # Core lineage analysis using SQLGlot
│   └── formatters.py         # Output formatters (text, JSON, CSV)
└── utils/
    └── file_utils.py         # File I/O utilities
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for complete project structure and component details.

## CLI Usage

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

## Development Guidelines

### Code Style
- Use Pydantic models for data structures (not dataclasses)
- Type hints required for all functions
- Docstrings for all public APIs
- Follow PEP 8 conventions
- Keep functions focused and small

### When Making Changes
1. **Update ARCHITECTURE.md** if you change:
   - Project structure (new modules, files, or directories)
   - Core components or their responsibilities
   - Key design decisions or algorithms
   - Technology stack or dependencies
   - CLI interface or command structure
2. Update this file (CLAUDE.md) if you change development workflows
3. Update README.md if you change user-facing features
4. Run linter before committing: `uv run ruff check --fix && uv run ruff format`

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
│   ├── lineage/
│   │   ├── __init__.py
│   │   ├── test_analyzer.py          # Tests for analyzer.py
│   │   └── test_formatters.py        # Tests for formatters.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── test_file_utils.py        # Tests for file_utils.py
│   └── test_cli.py                   # Tests for cli.py
└── fixtures/
    └── sample_queries.sql            # Shared test SQL files
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
    results = analyzer.analyze_column_lineage()

    assert len(results) == 1
    assert results[0].output_column == "orders.customer_id"
    assert results[0].source_columns == ["orders.customer_id"]
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
        analyzer.analyze_column_lineage()
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
   - Example: `plans/2024-12-05-column-level-lineage.md`
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
