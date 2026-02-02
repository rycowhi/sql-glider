---
icon: lucide/database
---

# Catalogs

SQL Glider can connect to remote data catalogs to pull DDL definitions for the tables referenced in your SQL. This is useful when you want to enrich lineage analysis with real schema information, validate queries against production table definitions, or simply collect DDL for documentation.

## How It Works

The `tables pull` command reads your SQL, extracts the table names, then fetches the `CREATE TABLE` DDL for each one from a remote catalog. CTEs are automatically excluded since they don't exist in any remote catalog.

```bash
# Pull DDL for all tables referenced in a query
sqlglider tables pull query.sql --catalog-type databricks
```

Output:

```sql
-- Table: my_catalog.my_schema.customers
CREATE TABLE my_catalog.my_schema.customers (
    customer_id BIGINT,
    name STRING,
    email STRING,
    created_at TIMESTAMP
)
USING DELTA;
```

You can also write each table's DDL to a separate file:

```bash
sqlglider tables pull query.sql -c databricks -o ./ddl/
```

This creates one `.sql` file per table in the `./ddl/` directory.

## Built-in: Databricks

SQL Glider ships with a Databricks Unity Catalog provider. It requires the Databricks SDK as an optional dependency:

```bash
pip install sql-glider[databricks]
```

### Authentication

The Databricks catalog uses the Databricks SDK's unified authentication, which tries the following sources in order:

1. Explicit host and token from `sqlglider.toml`
2. Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`)
3. Databricks CLI profile (`~/.databrickscfg`)
4. OAuth M2M via `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`
5. Azure CLI or Google Cloud auth for cloud-hosted workspaces

A **warehouse ID** is always required. Set it in config or via the `DATABRICKS_WAREHOUSE_ID` environment variable.

### Configuration

Configure Databricks in `sqlglider.toml`:

```toml
[sqlglider]
catalog_type = "databricks"

[sqlglider.catalog.databricks]
warehouse_id = "abc123def456"
profile = "my-workspace"       # optional: Databricks CLI profile
host = "https://my.databricks.com"  # optional if using profile or env vars
token = "dapi..."              # optional: prefer OAuth or profile instead
```

Or use environment variables:

```bash
export DATABRICKS_HOST="https://my.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="abc123def456"
```

### Usage

```bash
# Pull DDL to stdout
sqlglider tables pull query.sql -c databricks

# Pull DDL to folder
sqlglider tables pull query.sql -c databricks -o ./ddl/

# Combine with templating
sqlglider tables pull query.sql -c databricks --templater jinja --var schema=prod

# From stdin
echo "SELECT * FROM my_catalog.my_schema.users" | sqlglider tables pull -c databricks

# List available catalog providers
sqlglider tables pull --list
```

## Writing a Custom Catalog Provider

You can create your own catalog provider as a Python package and register it as a plugin.

### 1. Implement the Catalog Class

Subclass `sqlglider.catalog.base.Catalog` and implement three methods:

```python
from typing import Any, Dict, List, Optional

from sqlglider.catalog.base import Catalog, CatalogError


class SnowflakeCatalog(Catalog):
    @property
    def name(self) -> str:
        return "snowflake"

    def configure(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Set up connection details from config or environment."""
        config = config or {}
        self._account = config.get("account")
        self._warehouse = config.get("warehouse")
        # Validate required settings
        if not self._account:
            raise CatalogError("Snowflake account is required")

    def get_ddl(self, table_name: str) -> str:
        """Fetch DDL for a single table."""
        try:
            # Connect and run: GET_DDL('TABLE', table_name)
            return ddl_string
        except Exception as e:
            raise CatalogError(f"Failed to fetch DDL for {table_name}: {e}") from e

    def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
        """Fetch DDL for multiple tables.

        Return a dict mapping table names to DDL strings.
        For tables that fail, prefix the value with "ERROR: ".
        """
        results: Dict[str, str] = {}
        for table in table_names:
            try:
                results[table] = self.get_ddl(table)
            except CatalogError as e:
                results[table] = f"ERROR: {e}"
        return results
```

Key points:

- **`name`** — the identifier users pass to `--catalog-type`
- **`configure()`** — called after instantiation with settings from `sqlglider.toml` and CLI; validate required config here
- **`get_ddl()`** — fetch DDL for one table; raise `CatalogError` on failure
- **`get_ddl_batch()`** — fetch DDL for many tables; prefix failures with `"ERROR: "` so the batch continues

### 2. Register via Entry Points

In your package's `pyproject.toml`:

```toml
[project.entry-points."sqlglider.catalogs"]
snowflake = "my_package.catalog:SnowflakeCatalog"
```

### 3. Add Configuration Support

Users can configure your catalog in `sqlglider.toml`. The config section name matches the catalog name:

```toml
[sqlglider]
catalog_type = "snowflake"

[sqlglider.catalog.snowflake]
account = "xy12345.us-east-1"
warehouse = "COMPUTE_WH"
```

!!! note
    For the config section to be loaded automatically, the core `ConfigSettings` model needs to know about your provider. For third-party plugins, users can also pass configuration through environment variables in your `configure()` method.

### 4. Use It

After installing your package:

```bash
sqlglider tables pull query.sql --catalog-type snowflake
```
