"""Base classes for SQL templating system.

This module defines the abstract interface for templaters and provides
a no-op implementation that passes SQL through unchanged.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional


class TemplaterError(Exception):
    """Exception raised when templating fails."""

    pass


class Templater(ABC):
    """Abstract base class for SQL templaters.

    All templater implementations must inherit from this class and implement
    the required methods. Templaters are discovered via entry points and
    can be used to process SQL files before analysis.

    Example:
        >>> class MyTemplater(Templater):
        ...     @property
        ...     def name(self) -> str:
        ...         return "my-templater"
        ...
        ...     def render(self, sql, variables=None, source_path=None):
        ...         # Custom templating logic
        ...         return processed_sql
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the templater name.

        This name is used to identify the templater in configuration
        and CLI options.

        Returns:
            The unique name of this templater.
        """
        pass

    @abstractmethod
    def render(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        source_path: Optional[Path] = None,
    ) -> str:
        """Render a SQL template string.

        Args:
            sql: The SQL template string to render.
            variables: Template variables to substitute. Keys are variable
                      names, values can be any type supported by the templater.
            source_path: Optional path to the source file. Used for resolving
                        relative paths in includes/extends directives.

        Returns:
            The rendered SQL string with all template expressions evaluated.

        Raises:
            TemplaterError: If templating fails due to syntax errors,
                           missing variables, or other issues.
        """
        pass


class NoOpTemplater(Templater):
    """A templater that passes SQL through unchanged.

    This is the default templater when no templating is needed.
    It simply returns the input SQL without any processing.
    """

    @property
    def name(self) -> str:
        """Return the templater name."""
        return "none"

    def render(
        self,
        sql: str,
        variables: Optional[Dict[str, Any]] = None,
        source_path: Optional[Path] = None,
    ) -> str:
        """Pass SQL through unchanged.

        Args:
            sql: The SQL string.
            variables: Ignored for no-op templater.
            source_path: Ignored for no-op templater.

        Returns:
            The input SQL unchanged.
        """
        return sql
