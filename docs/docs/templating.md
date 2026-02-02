---
icon: lucide/braces
---

# SQL Templating

SQL Glider supports templating for SQL files, letting you parameterize queries with variables, conditionals, and loops before running any analysis. This is useful when your SQL references environment-specific schemas, toggleable columns, or other dynamic values.

## Quick Start

Given a SQL file `query.sql`:

```sql
SELECT * FROM {{ schema }}.customers
```

Render it with:

```bash
sqlglider template query.sql --var schema=analytics
```

Output:

```sql
SELECT * FROM analytics.customers
```

Templating works with all commands — not just `template`. Pass `--templater jinja` to `lineage`, `tables overview`, `graph build`, `dissect`, and others:

```bash
sqlglider lineage query.sql --templater jinja --var schema=analytics
```

## Built-in Templaters

SQL Glider ships with two templaters:

| Name | Description |
|------|-------------|
| `jinja` | Full Jinja2 templating (default for `template` command) |
| `none` | Pass-through, returns SQL unchanged |

List available templaters (including any installed plugins):

```bash
sqlglider template --list
```

## Jinja2 Syntax

The built-in Jinja templater supports the full Jinja2 feature set.

### Variables

```sql
SELECT * FROM {{ schema }}.{{ table }}
WHERE region = '{{ region }}'
```

### Conditionals

```sql
SELECT
    customer_id,
    name
    {% if include_email %}, email{% endif %}
FROM customers
```

### Loops

```sql
SELECT
    {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
FROM users
```

### Comments

```sql
{# This comment won't appear in the rendered SQL #}
SELECT * FROM orders
```

### Includes

Templates can include other SQL files. Paths resolve relative to the source file's directory:

```sql
{% include 'common_cte.sql' %}
SELECT * FROM common_cte
```

## Providing Variables

Variables can come from four sources. When the same variable is defined in multiple sources, higher-priority sources win.

**Priority (highest to lowest):**

1. CLI arguments (`--var key=value`)
2. Variables file (`--vars-file vars.json`)
3. Config file (`sqlglider.toml` inline variables)
4. Environment variables (`SQLGLIDER_VAR_*`)

### CLI Arguments

```bash
sqlglider template query.sql --var schema=prod --var limit=100
```

Values are type-inferred: `true`/`false` become booleans, numeric strings become integers or floats, everything else stays a string.

### Variables File

JSON, YAML, or TOML files are supported:

```bash
sqlglider template query.sql --vars-file vars.json
```

```json
{
  "schema": "analytics",
  "columns": ["id", "name", "email"],
  "include_email": true
}
```

### Config File

Set defaults in `sqlglider.toml`:

```toml
[sqlglider]
templater = "jinja"

[sqlglider.templating]
variables_file = "vars.json"

[sqlglider.templating.variables]
schema = "default_schema"
```

### Environment Variables

Any environment variable prefixed with `SQLGLIDER_VAR_` is available as a template variable (prefix stripped, name lowercased):

```bash
export SQLGLIDER_VAR_SCHEMA=prod
sqlglider template query.sql
# {{ schema }} resolves to "prod"
```

## Piping and Chaining

Templating works with stdin, so you can chain commands:

```bash
# Render a template then analyze lineage
cat query.sql | sqlglider template --var schema=prod | sqlglider lineage

# Inline SQL
echo "SELECT * FROM {{ schema }}.users" | sqlglider template --var schema=prod
```

## Writing a Custom Templater

You can create your own templater as a Python package and register it as a plugin.

### 1. Implement the Templater Class

Subclass `sqlglider.templating.base.Templater` and implement `name` and `render`:

```python
from pathlib import Path
from typing import Any, Dict, Optional

from sqlglider.templating.base import Templater, TemplaterError


class DbtStyleTemplater(Templater):
    @property
    def name(self) -> str:
        return "dbt-style"

    def render(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        source_path: Optional[Path] = None,
    ) -> str:
        variables = variables or {}
        try:
            # Your custom rendering logic here
            for key, value in variables.items():
                sql = sql.replace(f"{{{{ var('{key}') }}}}", str(value))
            return sql
        except Exception as e:
            raise TemplaterError(f"dbt-style templater error: {e}") from e
```

The `render` method receives:

- **`sql`** — the raw SQL string to process
- **`variables`** — merged dictionary from all variable sources (already resolved by priority)
- **`source_path`** — path to the SQL file, useful for resolving relative includes

Raise `TemplaterError` on failure so SQL Glider can report it cleanly.

### 2. Register via Entry Points

In your package's `pyproject.toml`, add an entry point under the `sqlglider.templaters` group:

```toml
[project.entry-points."sqlglider.templaters"]
dbt-style = "my_package.templater:DbtStyleTemplater"
```

The key (`dbt-style`) is the name users pass to `--templater`.

### 3. Use It

After installing your package, the templater appears in `--list` and can be used like any built-in:

```bash
sqlglider template query.sql --templater dbt-style --var schema=prod
```

Or set it as the default in `sqlglider.toml`:

```toml
[sqlglider]
templater = "dbt-style"
```
