"""SQL templating system for SQL Glider.

This package provides a plugin-based templating system for processing
SQL templates before analysis. It supports multiple templating engines
through a plugin architecture based on Python entry points.

Built-in templaters:
- `none`: No-op templater that passes SQL through unchanged
- `jinja`: Jinja2-based templater with full template syntax support

Example:
    >>> from sqlglider.templating import get_templater
    >>> templater = get_templater("jinja")
    >>> sql = "SELECT * FROM {{ schema }}.{{ table }}"
    >>> rendered = templater.render(sql, {"schema": "public", "table": "users"})
    >>> print(rendered)
    SELECT * FROM public.users
"""

from sqlglider.templating.base import NoOpTemplater, Templater, TemplaterError
from sqlglider.templating.registry import (
    clear_registry,
    get_templater,
    list_templaters,
    register_templater,
)
from sqlglider.templating.variables import (
    load_all_variables,
    load_env_variables,
    load_variables_file,
    merge_variables,
    parse_cli_variables,
)

__all__ = [
    # Base classes
    "Templater",
    "TemplaterError",
    "NoOpTemplater",
    # Registry functions
    "get_templater",
    "list_templaters",
    "register_templater",
    "clear_registry",
    # Variable loading
    "load_all_variables",
    "load_variables_file",
    "parse_cli_variables",
    "load_env_variables",
    "merge_variables",
]
