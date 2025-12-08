"""Templater registry with plugin discovery via entry points.

This module handles discovering and instantiating templaters from
Python entry points, allowing third-party packages to register
custom templaters.
"""

import sys
from typing import Dict, List, Type

from sqlglider.templating.base import Templater, TemplaterError

# Cache for discovered templaters
_templater_cache: Dict[str, Type[Templater]] = {}
_discovery_done: bool = False


def _discover_templaters() -> None:
    """Discover templaters from entry points.

    Uses importlib.metadata to find all registered templaters
    in the 'sqlglider.templaters' entry point group.
    """
    global _discovery_done, _templater_cache

    if _discovery_done:
        return

    if sys.version_info >= (3, 10):
        from importlib.metadata import entry_points

        eps = entry_points(group="sqlglider.templaters")
    else:
        from importlib.metadata import entry_points

        all_eps = entry_points()
        eps = all_eps.get("sqlglider.templaters", [])

    for ep in eps:
        try:
            templater_class = ep.load()
            if isinstance(templater_class, type) and issubclass(
                templater_class, Templater
            ):
                _templater_cache[ep.name] = templater_class
        except Exception:
            # Skip templaters that fail to load
            # This allows graceful handling of missing optional dependencies
            pass

    _discovery_done = True


def get_templater(name: str) -> Templater:
    """Get a templater instance by name.

    Args:
        name: The name of the templater (e.g., "jinja", "none").

    Returns:
        An instance of the requested templater.

    Raises:
        TemplaterError: If the templater is not found.

    Example:
        >>> templater = get_templater("jinja")
        >>> rendered = templater.render("SELECT * FROM {{ table }}", {"table": "users"})
    """
    _discover_templaters()

    if name not in _templater_cache:
        available = ", ".join(sorted(_templater_cache.keys()))
        raise TemplaterError(
            f"Unknown templater '{name}'. Available templaters: {available or 'none'}"
        )

    return _templater_cache[name]()


def list_templaters() -> List[str]:
    """List all available templater names.

    Returns:
        A sorted list of available templater names.

    Example:
        >>> templaters = list_templaters()
        >>> print(templaters)
        ['jinja', 'none']
    """
    _discover_templaters()
    return sorted(_templater_cache.keys())


def register_templater(name: str, templater_class: Type[Templater]) -> None:
    """Register a templater programmatically.

    This is primarily useful for testing or for registering templaters
    that aren't installed via entry points.

    Args:
        name: The name to register the templater under.
        templater_class: The templater class to register.

    Raises:
        ValueError: If templater_class is not a subclass of Templater.
    """
    if not isinstance(templater_class, type) or not issubclass(
        templater_class, Templater
    ):
        raise ValueError(f"{templater_class} must be a subclass of Templater")

    _templater_cache[name] = templater_class


def clear_registry() -> None:
    """Clear the templater registry.

    This is primarily useful for testing.
    """
    global _discovery_done, _templater_cache
    _templater_cache.clear()
    _discovery_done = False
