"""Catalog module for fetching DDL from remote data catalogs.

This module provides a plugin system for connecting to various data catalogs
(e.g., Databricks Unity Catalog) and fetching table DDL definitions.

Example:
    >>> from sqlglider.catalog import get_catalog, list_catalogs
    >>> print(list_catalogs())
    ['databricks']
    >>> catalog = get_catalog("databricks")
    >>> catalog.configure({"warehouse_id": "abc123"})
    >>> ddl = catalog.get_ddl("my_catalog.my_schema.my_table")
"""

from sqlglider.catalog.base import Catalog, CatalogError
from sqlglider.catalog.registry import (
    clear_registry,
    get_catalog,
    list_catalogs,
    register_catalog,
)

__all__ = [
    "Catalog",
    "CatalogError",
    "get_catalog",
    "list_catalogs",
    "register_catalog",
    "clear_registry",
]
