# Implementation Plan: SQL Templating System with Plugin Architecture

**Status:** Completed
**Date:** 2025-12-07
**Completed:** 2025-12-07

## Overview

Add a SQL templating system to SQL Glider that processes template variables in SQL files before analysis. The system will use a plugin architecture to support multiple templating engines, with Jinja2 as the first-class built-in implementation.

## Requirements

1. **New CLI command**: `sqlglider template` - outputs templated SQL to stdout or file
2. **Integration with existing commands**: Add `--templater` option to `lineage` and `graph` commands
3. **Plugin system**: Support custom templaters via entry points
4. **Configuration**: `templater` option settable in `sqlglider.toml`
5. **Jinja2 support**: First-class built-in templater

## Design Decisions

### 1. Plugin Architecture

**Use Python Entry Points** for plugin discovery:
- Standard Python mechanism for plugin systems
- Zero external dependencies for core plugin infrastructure
- Allows third-party templater implementations
- Built-in templaters registered via the same mechanism

Entry point group: `sqlglider.templaters`

```toml
# pyproject.toml
[project.entry-points."sqlglider.templaters"]
jinja = "sqlglider.templating.jinja:JinjaTemplater"
none = "sqlglider.templating.base:NoOpTemplater"
```

### 2. Templater Interface

**Abstract base class approach** (Protocol would also work):

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path

class Templater(ABC):
    """Base class for SQL templaters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the templater name."""
        pass

    @abstractmethod
    def render(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        source_path: Optional[Path] = None
    ) -> str:
        """
        Render a SQL template string.

        Args:
            sql: The SQL template string
            variables: Template variables to substitute
            source_path: Optional path to source file (for includes/extends)

        Returns:
            The rendered SQL string
        """
        pass
```

### 3. Variable Sources (Priority Order)

Variables for templating will be loaded from multiple sources with clear priority:

1. **CLI arguments**: `--var key=value` (highest priority)
2. **Variables file**: `--vars-file vars.json` or `--vars-file vars.yaml`
3. **Config file**: `[sqlglider.templating.variables]` section
4. **Environment variables**: `SQLGLIDER_VAR_*` prefix

### 4. Template Variables File Format

Support JSON and YAML (YAML requires optional dependency):

```json
{
  "schema": "analytics",
  "table_prefix": "stg_",
  "date": "2024-01-01"
}
```

```yaml
schema: analytics
table_prefix: stg_
date: "2024-01-01"
```

### 5. Jinja2 Templater Features

The built-in Jinja2 templater will support:
- Variable substitution: `{{ schema }}.{{ table }}`
- Conditionals: `{% if condition %}...{% endif %}`
- Loops: `{% for item in items %}...{% endfor %}`
- Includes: `{% include 'common.sql' %}` (relative to source file)
- Custom SQL-friendly delimiters option (to avoid conflicts)

### 6. Configuration Schema Extension

```toml
[sqlglider]
dialect = "spark"
level = "column"
output_format = "text"
templater = "jinja"  # NEW: default templater

[sqlglider.templating]  # NEW section
variables_file = "vars.json"  # Optional default variables file

[sqlglider.templating.variables]  # NEW: inline variables
schema = "analytics"
table_prefix = "stg_"
```

### 7. CLI Command Design

#### New `template` Command

```bash
# Basic usage - output to stdout
sqlglider template query.sql

# With variables
sqlglider template query.sql --var schema=analytics --var date=2024-01-01

# With variables file
sqlglider template query.sql --vars-file vars.json

# Output to file
sqlglider template query.sql -o rendered.sql

# Specify templater
sqlglider template query.sql --templater jinja
```

#### Updated `lineage` Command

```bash
# Enable templating before analysis
sqlglider lineage query.sql --templater jinja

# With template variables
sqlglider lineage query.sql --templater jinja --var schema=analytics

# Disable templating explicitly
sqlglider lineage query.sql --templater none
```

#### Updated `graph build` Command

```bash
# Build graph with templating
sqlglider graph build ./queries/ -o graph.json --templater jinja --vars-file vars.json
```

## Implementation Steps

### Phase 1: Create Templating Module Structure

**File:** `src/sqlglider/templating/__init__.py` (NEW)

- [x] Create package with exports
- [x] Export `Templater`, `get_templater`, `list_templaters`

**File:** `src/sqlglider/templating/base.py` (NEW)

- [x] Create `Templater` abstract base class
- [x] Create `NoOpTemplater` (passes through SQL unchanged)
- [x] Create `TemplaterError` exception class

**File:** `src/sqlglider/templating/registry.py` (NEW)

- [x] Implement `get_templater(name: str) -> Templater`
- [x] Implement `list_templaters() -> List[str]`
- [x] Use `importlib.metadata.entry_points()` for plugin discovery
- [x] Cache discovered templaters

### Phase 2: Implement Jinja2 Templater

**File:** `src/sqlglider/templating/jinja.py` (NEW)

- [x] Implement `JinjaTemplater(Templater)`
- [x] Support standard Jinja2 syntax
- [x] Handle `FileNotFoundError` for includes gracefully
- [x] Support relative paths for includes based on source file
- [x] Implement custom undefined handling (raise error vs. leave placeholder)

**Dependency:** Add `jinja2` to project dependencies

### Phase 3: Implement Variable Loading

**File:** `src/sqlglider/templating/variables.py` (NEW)

- [x] Implement `load_variables_file(path: Path) -> Dict[str, Any]`
  - Support `.json` files
  - Support `.yaml`/`.yml` files (with optional PyYAML dependency)
- [x] Implement `parse_cli_variables(vars: List[str]) -> Dict[str, Any]`
  - Parse `key=value` format
  - Support type inference (string, int, float, bool)
- [x] Implement `load_env_variables() -> Dict[str, Any]`
  - Load from `SQLGLIDER_VAR_*` environment variables
- [x] Implement `merge_variables(*sources) -> Dict[str, Any]`
  - Merge with priority order

### Phase 4: Update Configuration System

**File:** `src/sqlglider/utils/config.py`

- [x] Add `templater: Optional[str] = None` to `ConfigSettings`
- [x] Add `variables_file: Optional[str] = None` to `ConfigSettings`
- [x] Add `variables: Optional[Dict[str, Any]] = None` for inline variables

### Phase 5: Implement `template` CLI Command

**File:** `src/sqlglider/cli.py`

- [x] Add `template` command with options:
  - `sql_file: Path` (required)
  - `--templater / -t: str` (default from config or "jinja")
  - `--var: List[str]` (repeatable)
  - `--vars-file: Optional[Path]`
  - `--output-file / -o: Optional[Path]`
- [x] Implement command logic:
  1. Load config
  2. Load variables (merge from all sources)
  3. Get templater instance
  4. Read SQL file
  5. Render template
  6. Output to stdout or file

### Phase 6: Integrate Templating with `lineage` Command

**File:** `src/sqlglider/cli.py`

- [x] Add `--templater` option to `lineage` command
- [x] Add `--var` option (repeatable)
- [x] Add `--vars-file` option
- [x] Update `lineage` command to:
  1. Check if templater is specified (CLI or config)
  2. If templater != "none", render SQL before analysis
  3. Pass rendered SQL to `LineageAnalyzer`

### Phase 7: Integrate Templating with `graph build` Command

**File:** `src/sqlglider/cli.py`

- [x] Add `--templater` option to `graph build` command
- [x] Add `--var` option (repeatable)
- [x] Add `--vars-file` option
- [x] Update `GraphBuilder` or file processing to template before analysis

### Phase 8: Register Entry Points

**File:** `pyproject.toml`

- [x] Add entry points section:
  ```toml
  [project.entry-points."sqlglider.templaters"]
  jinja = "sqlglider.templating.jinja:JinjaTemplater"
  none = "sqlglider.templating.base:NoOpTemplater"
  ```
- [x] Add jinja2 dependency:
  ```toml
  dependencies = [
      ...
      "jinja2>=3.0.0",
  ]
  ```

### Phase 9: Write Unit Tests

**File:** `tests/sqlglider/templating/__init__.py` (NEW)

**File:** `tests/sqlglider/templating/test_base.py` (NEW)

- [x] Test `Templater` ABC
- [x] Test `NoOpTemplater` passes through unchanged
- [x] Test `TemplaterError` exception

**File:** `tests/sqlglider/templating/test_jinja.py` (NEW)

- [x] Test basic variable substitution
- [x] Test conditionals
- [x] Test loops
- [x] Test includes (with fixtures)
- [x] Test undefined variable handling
- [x] Test error messages

**File:** `tests/sqlglider/templating/test_variables.py` (NEW)

- [x] Test JSON file loading
- [x] Test YAML file loading
- [x] Test CLI variable parsing
- [x] Test environment variable loading
- [x] Test variable merging priority

**File:** `tests/sqlglider/templating/test_registry.py` (NEW)

- [x] Test `get_templater()` returns correct instance
- [x] Test `list_templaters()` returns available templaters
- [x] Test invalid templater name raises error

### Phase 10: Write Integration Tests

**File:** `tests/sqlglider/test_cli.py`

- [x] Test `template` command basic usage
- [x] Test `template` command with variables
- [x] Test `template` command output to file
- [x] Test `lineage` command with templating
- [x] Test `lineage` command templating disabled
- [x] Test `graph build` with templating
- [x] Test config file templater option

### Phase 11: Update Documentation

**File:** `ARCHITECTURE.md`

- [ ] Add "Templating System" section (skipped - not requested)
- [ ] Document plugin architecture (skipped - not requested)
- [ ] Document templater interface (skipped - not requested)
- [ ] Document variable loading priority (skipped - not requested)
- [ ] Add design decision (skipped - not requested)

**File:** `CLAUDE.md`

- [x] Add templating CLI usage examples
- [x] Document how to create custom templaters
- [x] Update configuration file format

**File:** `sqlglider.toml.example`

- [x] Add templater option
- [x] Add templating section with variables

**File:** `README.md`

- [ ] Add templating section (skipped - not requested)
- [ ] Show basic examples (skipped - not requested)

## Critical Files to Create

1. `src/sqlglider/templating/__init__.py` - Package exports
2. `src/sqlglider/templating/base.py` - Base class and NoOp
3. `src/sqlglider/templating/jinja.py` - Jinja2 implementation
4. `src/sqlglider/templating/variables.py` - Variable loading
5. `src/sqlglider/templating/registry.py` - Plugin discovery
6. `tests/sqlglider/templating/` - All test files

## Critical Files to Modify

1. `src/sqlglider/cli.py` - Add template command, update lineage/graph
2. `src/sqlglider/utils/config.py` - Add templater config
3. `pyproject.toml` - Add dependency, entry points
4. `ARCHITECTURE.md` - Documentation
5. `CLAUDE.md` - Documentation
6. `sqlglider.toml.example` - Configuration example

## Testing Strategy

### Unit Tests

1. **Templater Base** - ABC, NoOp, exceptions
2. **Jinja Templater** - All Jinja2 features
3. **Variable Loading** - All sources and merging
4. **Registry** - Plugin discovery and instantiation

### Integration Tests

1. **CLI Commands** - All new/modified commands
2. **Config Integration** - Config file with templater options
3. **End-to-end** - Template + lineage analysis

### Coverage Requirements

- New modules: >80%
- Overall project: Maintain >80%

## Example Usage

### Template Command

```bash
# Simple variable substitution
echo "SELECT * FROM {{ schema }}.{{ table }}" > query.sql
sqlglider template query.sql --var schema=analytics --var table=users
# Output: SELECT * FROM analytics.users

# With variables file
cat > vars.json << EOF
{"schema": "analytics", "table": "users"}
EOF
sqlglider template query.sql --vars-file vars.json
```

### Lineage with Templating

```bash
# Analyze templated SQL
cat > query.sql << EOF
SELECT
    customer_id,
    {{ agg_function }}(order_total) as total
FROM {{ schema }}.orders
GROUP BY customer_id
EOF

sqlglider lineage query.sql --templater jinja \
    --var schema=sales --var agg_function=SUM
```

### Configuration File

```toml
# sqlglider.toml
[sqlglider]
dialect = "snowflake"
templater = "jinja"

[sqlglider.templating.variables]
schema = "production"
environment = "prod"
```

## Success Criteria

- [x] `sqlglider template` command works with basic Jinja2 syntax
- [x] `--templater` option works on `lineage` command
- [x] `--templater` option works on `graph build` command
- [x] Variables can be passed via CLI, file, and config
- [x] Jinja2 templater supports variables, conditionals, loops
- [x] Plugin system allows custom templaters via entry points
- [x] Configuration file supports `templater` option
- [x] All tests pass with >80% coverage (378 passed, 85.57% coverage)
- [x] Documentation updated

## Implementation Notes

### Deviations from Original Plan
- Added `_apply_templating()` helper function in CLI to reduce code duplication (per user feedback)
- Used Jinja2's built-in `StrictUndefined` instead of custom class (simpler, avoids subclass issues)
- Added `sql_preprocessor` parameter to `GraphBuilder` for clean templating integration

### Technical Challenges
1. **Custom `_StrictUndefined` class**: Initially created a custom Undefined subclass, but Jinja2's Environment requires it to be a proper subclass of `jinja2.Undefined`. Resolved by using the built-in `StrictUndefined`.
2. **Include file tests**: Tests for `{% include %}` directive initially failed because the main SQL file wasn't written to disk, causing `source_path.is_file()` to return False. Fixed by ensuring tests create both the main and include files.

### Test Results
- **378 tests passed**, 4 skipped
- **85.57% code coverage** (above 80% threshold)

## Future Enhancements

1. **Additional built-in templaters**: dbt-style `{{ ref('table') }}`, SQLFluff templating
2. **Template validation**: `sqlglider template --validate` to check syntax without rendering
3. **Variable type coercion**: Support lists, dicts in variables
4. **Template caching**: Cache rendered templates for performance
5. **Macro libraries**: Support for shared macros across files
