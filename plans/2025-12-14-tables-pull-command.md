# Plan: Add `tables pull` Command for Remote DDL Fetching

**Status:** Completed

## Overview

Add a new `sqlglider tables pull` command that extracts tables used in a SQL file and fetches their DDL definitions from a remote catalog. Uses a plugin system (similar to templating) to support multiple catalog providers, starting with Databricks.

## Requirements

1. Command: `sqlglider tables pull sqlfile.sql`
2. `--catalog-type` option (inferred from `sqlglider.toml`, error if not provided)
3. `--ddl-folder` option for output directory (also configurable in `sqlglider.toml`)
4. DDL files named by full identifier: `schemaName.databaseName.tableName.sql`
5. If no `--ddl-folder`, output DDL statements to stdout
6. Support templating and variable options (like other commands)
7. Support stdin SQL input (like other commands)
8. Plugin system for catalog providers via entry points
9. **Automatically skip CTEs** when pulling DDL (they don't exist in remote catalogs)

## Decisions

- **Databricks SDK**: Use `databricks-sdk` as an **optional dependency** (`pip install sql-glider[databricks]`)
- **CTE handling**: Automatically skip tables with `object_type=CTE` when pulling DDL
- **Authentication**: Support both environment variables AND `sqlglider.toml` config. Env vars use same names as the SDK (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`)

## Architecture

### New Directory Structure

```
src/sqlglider/catalog/
├── __init__.py           # Public API exports
├── base.py               # Abstract Catalog class + CatalogError exception
├── registry.py           # Plugin discovery via entry points
└── databricks.py         # Databricks catalog implementation
```

### Entry Points (pyproject.toml)

```toml
[project.optional-dependencies]
databricks = ["databricks-sdk>=0.20.0"]

[project.entry-points."sqlglider.catalogs"]
databricks = "sqlglider.catalog.databricks:DatabricksCatalog"
```

### Base Interface

```python
class CatalogError(Exception):
    """Exception raised when catalog operations fail."""
    pass

class Catalog(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Catalog provider name."""
        pass

    @abstractmethod
    def get_ddl(self, table_name: str) -> str:
        """Fetch DDL for a single table from the remote catalog."""
        pass

    @abstractmethod
    def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
        """Fetch DDL for multiple tables. Returns {table_name: ddl_string}."""
        pass
```

### Databricks Implementation

Uses `databricks-sdk` with `SHOW CREATE TABLE` via statement execution API:
- Authentication: env vars (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) or config file
- Warehouse ID required for SQL execution (env var `DATABRICKS_WAREHOUSE_ID` or config)

### Configuration (sqlglider.toml)

```toml
[sqlglider]
catalog_type = "databricks"
ddl_folder = "./ddl"

[sqlglider.catalog.databricks]
host = "https://my-workspace.cloud.databricks.com"
token = "dapi..."  # Or use env var DATABRICKS_TOKEN
warehouse_id = "abc123..."
```

### CLI Command

```python
@tables_app.command("pull")
def tables_pull(
    sql_file: Annotated[typer.FileText, typer.Argument(...)],
    catalog_type: Optional[str] = typer.Option(None, "--catalog-type", "-c", help="Catalog provider (e.g., 'databricks')"),
    ddl_folder: Optional[Path] = typer.Option(None, "--ddl-folder", "-o", help="Output folder for DDL files"),
    dialect: Optional[str] = typer.Option(None, "--dialect", "-d", help="SQL dialect"),
    templater: Optional[str] = typer.Option(None, "--templater", "-t", help="Templater for SQL preprocessing"),
    var: Optional[List[str]] = typer.Option(None, "--var", help="Template variable (key=value)"),
    vars_file: Optional[Path] = typer.Option(None, "--vars-file", help="Variables file (JSON/YAML)"),
) -> None:
    """Pull DDL definitions from a remote catalog for tables used in SQL."""
    ...
```

## Implementation Steps

- [x] 1. Create `src/sqlglider/catalog/__init__.py` with public exports
- [x] 2. Create `src/sqlglider/catalog/base.py` with `Catalog` ABC and `CatalogError`
- [x] 3. Create `src/sqlglider/catalog/registry.py` with plugin discovery (mirror templating pattern)
- [x] 4. Create `src/sqlglider/catalog/databricks.py` with `DatabricksCatalog` class
- [x] 5. Update `src/sqlglider/utils/config.py` to add `catalog_type`, `ddl_folder`, and `[sqlglider.catalog.*]` config
- [x] 6. Add `tables pull` command to `src/sqlglider/cli.py`
- [x] 7. Update `pyproject.toml` with entry point and optional `[databricks]` dependency
- [x] 8. Create `tests/sqlglider/catalog/` with unit tests
- [x] 9. Update `sqlglider.toml.example` with catalog config examples
- [x] 10. Update documentation (ARCHITECTURE.md, README.md, CLAUDE.md)

## Files to Create

| File | Purpose |
|------|---------|
| `src/sqlglider/catalog/__init__.py` | Public API: `Catalog`, `CatalogError`, `get_catalog`, `list_catalogs` |
| `src/sqlglider/catalog/base.py` | Abstract base class and exception |
| `src/sqlglider/catalog/registry.py` | Entry point discovery and catalog factory |
| `src/sqlglider/catalog/databricks.py` | Databricks implementation using `databricks-sdk` |
| `tests/sqlglider/catalog/__init__.py` | Test package |
| `tests/sqlglider/catalog/test_base.py` | Tests for base classes |
| `tests/sqlglider/catalog/test_registry.py` | Tests for registry |
| `tests/sqlglider/catalog/test_databricks.py` | Tests for Databricks (with mocking) |

## Files to Modify

| File | Changes |
|------|---------|
| `src/sqlglider/cli.py` | Add `tables_pull` command (~80 lines) |
| `src/sqlglider/utils/config.py` | Add `CatalogConfig`, `catalog_type`, `ddl_folder` fields |
| `pyproject.toml` | Add `[project.optional-dependencies]` and entry point |
| `sqlglider.toml.example` | Add catalog configuration examples |
| `ARCHITECTURE.md` | Document new catalog module |
| `README.md` | Add usage examples for `tables pull` |

## Testing Strategy

1. **Unit tests for base classes**: Test `Catalog` ABC contract
2. **Registry tests**: Test plugin discovery, `get_catalog()`, `list_catalogs()`
3. **Databricks tests with mocking**: Mock `WorkspaceClient` and statement execution
4. **CLI integration tests**: Test command with mocked catalog
5. **Error handling tests**: Missing config, auth failures, table not found
