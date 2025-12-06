# Implementation Plan: `sqlglider.toml` Configuration File Support

**Status:** Completed
**Date:** 2025-12-06
**Completion Date:** 2025-12-06

## Overview

Add support for a `sqlglider.toml` configuration file in the current working directory (PWD) that allows users to set default values for CLI options. CLI arguments will always override configuration file values, establishing a clear precedent for application-level configuration with command-line overrides.

## Requirements

- Load configuration from `sqlglider.toml` in the current working directory
- Support configurable options: `dialect`, `level`, `output_format`
- Priority order: **CLI arguments > TOML config > hardcoded defaults**
- Graceful error handling (malformed TOML, invalid values)
- Zero new external dependencies (use Python 3.11+ built-in `tomllib`)
- Set precedent for future configurable options

## Design Decisions

### 1. TOML Library
**Use Python's built-in `tomllib`** (read-only, available in Python 3.11+)
- No external dependencies needed
- Standard library reliability
- Read-only is sufficient (users edit config manually)

### 2. Config File Location
**Check `sqlglider.toml` in current working directory only**
- Simple to understand and implement
- Matches project-based tools (pyproject.toml, .gitignore)
- Future: Could add user-level config in `~/.config/sqlglider/`

### 3. TOML Schema Structure

```toml
[sqlglider]
# SQL dialect for parsing (default: "spark")
dialect = "postgres"

# Analysis level: "column" or "table" (default: "column")
level = "column"

# Output format: "text", "json", or "csv" (default: "text")
output_format = "json"
```

### 4. Error Handling Strategy
- **File not found:** Silently continue with defaults (config is optional)
- **Malformed TOML:** Warn user, continue with defaults
- **Invalid values:** Warn user, use defaults for invalid fields
- **Unknown keys:** Ignore (forward compatibility)

### 5. Priority Resolution
Use `None` as default in `typer.Option()`, then apply:
```python
dialect = cli_arg or config.dialect or "spark"
```

## Implementation Steps

### Phase 1: Create Configuration Module

**File:** `src/sqlglider/utils/config.py` (NEW)

- [ ] Create `ConfigSettings` Pydantic model with optional fields:
  - `dialect: Optional[str] = None`
  - `level: Optional[str] = None`
  - `output_format: Optional[str] = None`
- [ ] Implement `find_config_file(start_path: Optional[Path] = None) -> Optional[Path]`
  - Check for `sqlglider.toml` in PWD
  - Return Path if found, None otherwise
- [ ] Implement `load_config(config_path: Optional[Path] = None) -> ConfigSettings`
  - Use `tomllib.load()` to read TOML file
  - Handle `TOMLDecodeError` with user-friendly warning
  - Return `ConfigSettings` with values from TOML or None for unset
  - Always return valid ConfigSettings (fail gracefully)

**Dependencies:** `tomllib`, `pathlib`, `pydantic`

### Phase 2: Write Unit Tests

**File:** `tests/sqlglider/utils/test_config.py` (NEW)

- [ ] Test valid config loading with all fields
- [ ] Test partial config (some fields set)
- [ ] Test missing config file (returns empty ConfigSettings)
- [ ] Test malformed TOML (warns and returns empty)
- [ ] Test invalid value types (warns and sets to None)
- [ ] Test unknown keys (ignored)
- [ ] Test empty config file
- [ ] Test config with comments

**Coverage target:** >80%

### Phase 3: Update CLI Integration

**File:** `src/sqlglider/cli.py`

- [ ] Import config module: `from sqlglider.utils.config import load_config`
- [ ] Change `typer.Option()` defaults from hardcoded values to `None`:
  - `dialect: Optional[str] = typer.Option(None, ...)`
  - `level: Optional[str] = typer.Option(None, ...)`
  - `output_format: Optional[str] = typer.Option(None, ...)`
- [ ] Load config at start of `lineage()` function:
  ```python
  config = load_config()
  ```
- [ ] Apply priority resolution:
  ```python
  dialect = dialect or config.dialect or "spark"
  level = level or config.level or "column"
  output_format = output_format or config.output_format or "text"
  ```
- [ ] Update docstring to mention config file support

### Phase 4: Integration Tests

**File:** `tests/sqlglider/test_cli.py`

- [ ] Test CLI uses config defaults when no args provided
- [ ] Test CLI args override config values
- [ ] Test missing config uses hardcoded defaults
- [ ] Test partial config mixed with defaults
- [ ] Test priority order: CLI > config > default
- [ ] Test backward compatibility (CLI works without config)

**Use:** `typer.testing.CliRunner`, temporary config files, `monkeypatch.chdir()`

### Phase 5: Update Documentation

**File:** `ARCHITECTURE.md`

- [ ] Add "Configuration System" section after "File Utilities"
- [ ] Document `ConfigSettings` Pydantic model
- [ ] Document `find_config_file()` and `load_config()` functions
- [ ] Document configuration priority order
- [ ] Document error handling strategy
- [ ] Document TOML file format
- [ ] Add design decision: "Configuration File Support"

**File:** `CLAUDE.md`

- [ ] Add "Configuration Files" subsection under "Development Guidelines"
- [ ] Document how to add new configurable CLI options
- [ ] Provide TOML format example
- [ ] Explain priority order

**File:** `README.md` (if exists, or mention in user docs)

- [ ] Add "Configuration" section
- [ ] Show example `sqlglider.toml`
- [ ] Document supported options
- [ ] Explain priority order with examples

**File:** `sqlglider.toml.example` (NEW, project root)

- [ ] Create example config with all options documented
- [ ] Include comments explaining each option

### Phase 6: Module Exports

**File:** `src/sqlglider/utils/__init__.py`

- [ ] Add exports:
  ```python
  from .config import ConfigSettings, load_config, find_config_file
  ```

## Critical Files to Modify

1. **`src/sqlglider/utils/config.py`** (NEW) - Core config module
2. **`src/sqlglider/cli.py`** - CLI integration
3. **`tests/sqlglider/utils/test_config.py`** (NEW) - Unit tests
4. **`tests/sqlglider/test_cli.py`** - Integration tests
5. **`ARCHITECTURE.md`** - Architecture documentation
6. **`CLAUDE.md`** - Development guidelines

## Testing Strategy

### Unit Tests (config.py)
- Load valid/partial/empty configs
- Handle errors (missing file, malformed TOML, invalid types)
- Config file discovery
- Forward compatibility (unknown keys)

### Integration Tests (CLI)
- Config defaults used when no CLI args
- CLI args override config
- Priority resolution works correctly
- Backward compatibility maintained

### Coverage
- Config module: >80%
- Overall project: Maintain >80%

## Example Usage

### Before (CLI only)
```bash
uv run sqlglider lineage query.sql --dialect postgres --output-format json
```

### After (with config)
```toml
# sqlglider.toml
[sqlglider]
dialect = "postgres"
output_format = "json"
```

```bash
# Uses config defaults
uv run sqlglider lineage query.sql

# CLI overrides config
uv run sqlglider lineage query.sql --dialect snowflake
```

## Future Enhancements

- Config commands: `sqlglider config show` (display current config), `sqlglider config init` (generate template)
- Additional configurable options: `output_file`, `table_filter`, `column`, `source_column`
- Config profiles: `[sqlglider.profiles.dev]`, `[sqlglider.profiles.prod]` with `--profile` flag
- Config validation command: `sqlglider config validate`
- Environment variable for explicit config path: `SQLGLIDER_CONFIG` (overrides PWD search)

**Note:** Config files should always be project-specific (PWD only). No user-level or global configuration to maintain simplicity and project isolation.

## Success Criteria

- [x] Config file loads successfully from PWD
- [x] Supported options (dialect, level, output_format) are configurable
- [x] CLI arguments override config values
- [x] Missing or malformed config doesn't break CLI
- [x] All tests pass with >80% coverage (82.35% achieved)
- [x] Documentation updated in all relevant files
- [x] Zero new external dependencies
- [x] Backward compatible (CLI works without config file)

## Implementation Notes

### What Was Implemented

**Core Configuration System:**
- Created `src/sqlglider/utils/config.py` with `ConfigSettings` Pydantic model
- Implemented `find_config_file()` and `load_config()` functions
- Used Python 3.11+ built-in `tomllib` (zero dependencies)
- Graceful error handling for all edge cases

**CLI Integration:**
- Updated `src/sqlglider/cli.py` to load config on startup
- Changed all configurable options to use `None` as default
- Implemented priority resolution: CLI > config > defaults
- Updated help text to mention config file support

**Comprehensive Testing:**
- Created 25 unit tests in `tests/sqlglider/utils/test_config.py`
- Added 7 integration tests in `tests/sqlglider/test_cli.py`
- All 186 tests pass (185 passed, 1 skipped)
- Overall coverage: 82.35% (exceeds 80% requirement)

**Documentation:**
- Updated `ARCHITECTURE.md` with new "Configuration System" section
- Added design decision #7: "Configuration File Support"
- Updated `CLAUDE.md` with configuration guidelines
- Created `sqlglider.toml.example` with comprehensive examples
- Updated project structure diagrams in both files

### Technical Challenges Resolved

1. **Config File Discovery**: Implemented PWD-only search to maintain project isolation and simplicity
2. **Error Handling**: All config errors are non-fatal with warnings to stderr
3. **Priority Resolution**: Used Python's `or` operator for clean priority chain
4. **Testing Approach**: Used `TemporaryDirectory` and `os.chdir()` for integration tests

### Deviations from Original Plan

**Positive Deviations:**
- Added more comprehensive test coverage than initially planned (32 tests total for config)
- Implemented clearer warning messages with Rich console formatting
- Added forward compatibility by ignoring unknown keys in config

**Scope Clarifications:**
- User-level config (~/.config/sqlglider/) explicitly removed from future plans per user request
- Config remains project-specific (PWD only) to maintain simplicity

### Files Created

1. `src/sqlglider/utils/config.py` - 118 lines
2. `tests/sqlglider/utils/test_config.py` - 338 lines
3. `sqlglider.toml.example` - 52 lines

### Files Modified

1. `src/sqlglider/cli.py` - Added config loading and priority resolution
2. `src/sqlglider/utils/__init__.py` - Exported config functions
3. `tests/sqlglider/test_cli.py` - Added TestConfigIntegration class
4. `ARCHITECTURE.md` - Added configuration system documentation
5. `CLAUDE.md` - Added configuration guidelines

### Test Results

```
185 passed, 1 skipped in 5.65s
Coverage: 82.35% (exceeds 80% requirement)

New test coverage:
- src/sqlglider/utils/config.py: 92.31%
- All integration tests passing
```

### Known Limitations

None. All success criteria met.

### Future Improvements

The following enhancements are documented for future consideration:
- Config commands: `sqlglider config show`, `sqlglider config init`
- Config profiles: `[sqlglider.profiles.dev]`, `[sqlglider.profiles.prod]`
- Config validation: `sqlglider config validate`
- Additional options: `output_file`, `table_filter`, `column`, `source_column`
- Environment variable: `SQLGLIDER_CONFIG` for explicit path override

## Lessons Learned

1. **Python 3.11+ Built-ins**: Using `tomllib` avoided external dependencies and worked perfectly
2. **Pydantic Validation**: Unknown fields are ignored by default, providing forward compatibility
3. **Testing Strategy**: Temporary directories with `os.chdir()` worked well for PWD-based config
4. **Error Messaging**: Rich console formatting for warnings improved user experience
5. **Priority Resolution**: Simple `or` chaining (`cli or config or default`) is clean and maintainable
6. **Documentation**: Updating ARCHITECTURE.md and CLAUDE.md together ensures consistency
