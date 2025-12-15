"""Base classes for catalog system.

This module defines the abstract interface for catalog providers and provides
the exception class for catalog-related errors.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class CatalogError(Exception):
    """Exception raised when catalog operations fail."""

    pass


class Catalog(ABC):
    """Abstract base class for catalog providers.

    All catalog implementations must inherit from this class and implement
    the required methods. Catalogs are discovered via entry points and
    can be used to fetch DDL definitions from remote data catalogs.

    Example:
        >>> class MyCatalog(Catalog):
        ...     @property
        ...     def name(self) -> str:
        ...         return "my-catalog"
        ...
        ...     def get_ddl(self, table_name: str) -> str:
        ...         # Fetch DDL from remote catalog
        ...         return "CREATE TABLE ..."
        ...
        ...     def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
        ...         return {name: self.get_ddl(name) for name in table_names}
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the catalog provider name.

        This name is used to identify the catalog in configuration
        and CLI options.

        Returns:
            The unique name of this catalog provider.
        """
        pass

    @abstractmethod
    def get_ddl(self, table_name: str) -> str:
        """Fetch DDL for a single table from the remote catalog.

        Args:
            table_name: The fully qualified table name (e.g., "catalog.schema.table").

        Returns:
            The DDL statement for creating the table.

        Raises:
            CatalogError: If the DDL cannot be fetched (table not found,
                         authentication failure, network error, etc.).
        """
        pass

    @abstractmethod
    def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
        """Fetch DDL for multiple tables from the remote catalog.

        This method may be more efficient than calling get_ddl() multiple
        times, as implementations can batch requests where supported.

        Args:
            table_names: List of fully qualified table names.

        Returns:
            Dictionary mapping table names to their DDL statements.
            Tables that couldn't be found will have None as their value.

        Raises:
            CatalogError: If the batch operation fails entirely.
        """
        pass

    def configure(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Configure the catalog with provider-specific settings.

        This method is called after instantiation to pass configuration
        from sqlglider.toml or environment variables.

        Args:
            config: Provider-specific configuration dictionary.
                   Keys and values depend on the catalog implementation.

        Raises:
            CatalogError: If required configuration is missing or invalid.
        """
        pass
