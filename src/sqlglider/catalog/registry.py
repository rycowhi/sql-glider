"""Catalog registry with plugin discovery via entry points.

This module handles discovering and instantiating catalog providers from
Python entry points, allowing third-party packages to register
custom catalogs.
"""

import sys
from typing import Dict, List, Type

from sqlglider.catalog.base import Catalog, CatalogError

# Cache for discovered catalogs
_catalog_cache: Dict[str, Type[Catalog]] = {}
_discovery_done: bool = False


def _discover_catalogs() -> None:
    """Discover catalogs from entry points.

    Uses importlib.metadata to find all registered catalogs
    in the 'sqlglider.catalogs' entry point group.
    """
    global _discovery_done, _catalog_cache

    if _discovery_done:
        return

    if sys.version_info >= (3, 10):
        from importlib.metadata import entry_points

        eps = entry_points(group="sqlglider.catalogs")
    else:
        from importlib.metadata import entry_points

        all_eps = entry_points()
        eps = all_eps.get("sqlglider.catalogs", [])

    for ep in eps:
        try:
            catalog_class = ep.load()
            if isinstance(catalog_class, type) and issubclass(catalog_class, Catalog):
                _catalog_cache[ep.name] = catalog_class
        except Exception:
            # Skip catalogs that fail to load
            # This allows graceful handling of missing optional dependencies
            pass

    _discovery_done = True


def get_catalog(name: str) -> Catalog:
    """Get a catalog instance by name.

    Args:
        name: The name of the catalog (e.g., "databricks").

    Returns:
        An instance of the requested catalog.

    Raises:
        CatalogError: If the catalog is not found.

    Example:
        >>> catalog = get_catalog("databricks")
        >>> ddl = catalog.get_ddl("my_catalog.my_schema.my_table")
    """
    _discover_catalogs()

    if name not in _catalog_cache:
        available = ", ".join(sorted(_catalog_cache.keys()))
        raise CatalogError(
            f"Unknown catalog '{name}'. Available catalogs: {available or 'none'}. "
            f"You may need to install an optional dependency (e.g., pip install sql-glider[databricks])."
        )

    return _catalog_cache[name]()


def list_catalogs() -> List[str]:
    """List all available catalog names.

    Returns:
        A sorted list of available catalog names.

    Example:
        >>> catalogs = list_catalogs()
        >>> print(catalogs)
        ['databricks']
    """
    _discover_catalogs()
    return sorted(_catalog_cache.keys())


def register_catalog(name: str, catalog_class: Type[Catalog]) -> None:
    """Register a catalog programmatically.

    This is primarily useful for testing or for registering catalogs
    that aren't installed via entry points.

    Args:
        name: The name to register the catalog under.
        catalog_class: The catalog class to register.

    Raises:
        ValueError: If catalog_class is not a subclass of Catalog.
    """
    if not isinstance(catalog_class, type) or not issubclass(catalog_class, Catalog):
        raise ValueError(f"{catalog_class} must be a subclass of Catalog")

    _catalog_cache[name] = catalog_class


def clear_registry() -> None:
    """Clear the catalog registry.

    This is primarily useful for testing.
    """
    global _discovery_done, _catalog_cache
    _catalog_cache.clear()
    _discovery_done = False
