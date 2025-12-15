"""Databricks catalog implementation.

This module provides integration with Databricks Unity Catalog for fetching
table DDL definitions using the Databricks SDK.

Requires the optional 'databricks' dependency:
    pip install sql-glider[databricks]
"""

import os
from typing import Any, Dict, List, Optional

from sqlglider.catalog.base import Catalog, CatalogError

# Lazy import to avoid requiring databricks-sdk unless actually used
_databricks_sdk_available: Optional[bool] = None


def _check_databricks_sdk() -> None:
    """Check if databricks-sdk is installed."""
    global _databricks_sdk_available
    if _databricks_sdk_available is None:
        try:
            import databricks.sdk  # noqa: F401

            _databricks_sdk_available = True
        except ImportError:
            _databricks_sdk_available = False

    if not _databricks_sdk_available:
        raise CatalogError(
            "The 'databricks-sdk' package is required for Databricks catalog support. "
            "Install it with: pip install sql-glider[databricks]"
        )


class DatabricksCatalog(Catalog):
    """Databricks Unity Catalog provider.

    Fetches table DDL using the Databricks SDK's statement execution API.

    Authentication:
        Authentication is handled by the Databricks SDK, which supports:
        - Environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN
        - Databricks CLI profile (~/.databrickscfg)
        - Azure, GCP, or AWS authentication methods

    Configuration:
        - warehouse_id (required): SQL warehouse ID for statement execution
        - host (optional): Databricks workspace URL (can also use DATABRICKS_HOST env var)
        - token (optional): Databricks access token (can also use DATABRICKS_TOKEN env var)

    Example:
        >>> catalog = DatabricksCatalog()
        >>> catalog.configure({"warehouse_id": "abc123def456"})
        >>> ddl = catalog.get_ddl("my_catalog.my_schema.my_table")
    """

    def __init__(self) -> None:
        """Initialize the Databricks catalog."""
        self._warehouse_id: Optional[str] = None
        self._host: Optional[str] = None
        self._token: Optional[str] = None
        self._client: Any = None

    @property
    def name(self) -> str:
        """Return the catalog provider name."""
        return "databricks"

    def configure(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Configure the Databricks catalog.

        Args:
            config: Configuration dictionary with optional keys:
                - warehouse_id: SQL warehouse ID (required, or set DATABRICKS_WAREHOUSE_ID)
                - host: Databricks workspace URL (or set DATABRICKS_HOST)
                - token: Access token (or set DATABRICKS_TOKEN)

        Raises:
            CatalogError: If warehouse_id is not provided and not in environment.
        """
        config = config or {}

        # Get warehouse_id from config or environment
        self._warehouse_id = config.get("warehouse_id") or os.environ.get(
            "DATABRICKS_WAREHOUSE_ID"
        )
        if not self._warehouse_id:
            raise CatalogError(
                "Databricks warehouse_id is required. "
                "Set it in sqlglider.toml under [sqlglider.catalog.databricks] "
                "or via the DATABRICKS_WAREHOUSE_ID environment variable."
            )

        # Get optional host and token (SDK will also check env vars)
        self._host = config.get("host") or os.environ.get("DATABRICKS_HOST")
        self._token = config.get("token") or os.environ.get("DATABRICKS_TOKEN")

        # Reset client so it gets recreated with new config
        self._client = None

    def _get_client(self) -> Any:
        """Get or create the Databricks WorkspaceClient.

        Returns:
            The WorkspaceClient instance.

        Raises:
            CatalogError: If the SDK is not installed or configuration is invalid.
        """
        _check_databricks_sdk()

        if self._client is None:
            from databricks.sdk import WorkspaceClient

            # Build kwargs for WorkspaceClient
            kwargs: Dict[str, Any] = {}
            if self._host:
                kwargs["host"] = self._host
            if self._token:
                kwargs["token"] = self._token

            try:
                self._client = WorkspaceClient(**kwargs)
            except Exception as e:
                raise CatalogError(f"Failed to create Databricks client: {e}") from e

        return self._client

    def get_ddl(self, table_name: str) -> str:
        """Fetch DDL for a single table from Databricks.

        Uses SHOW CREATE TABLE to get the full DDL statement.

        Args:
            table_name: The fully qualified table name (catalog.schema.table).

        Returns:
            The CREATE TABLE DDL statement.

        Raises:
            CatalogError: If the table is not found or the query fails.
        """
        if not self._warehouse_id:
            raise CatalogError(
                "Catalog not configured. Call configure() with warehouse_id first."
            )

        client = self._get_client()

        try:
            # Execute SHOW CREATE TABLE statement
            response = client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=f"SHOW CREATE TABLE {table_name}",
                wait_timeout="30s",
            )

            # Check for errors
            if response.status and response.status.state:
                state = response.status.state.value
                if state == "FAILED":
                    error_msg = (
                        response.status.error.message
                        if response.status.error
                        else "Unknown error"
                    )
                    raise CatalogError(
                        f"Failed to get DDL for '{table_name}': {error_msg}"
                    )

            # Extract DDL from result
            if response.result and response.result.data_array:
                # SHOW CREATE TABLE returns a single row with the DDL
                ddl_parts = []
                for row in response.result.data_array:
                    if row:
                        ddl_parts.append(str(row[0]))
                return "\n".join(ddl_parts)

            raise CatalogError(f"No DDL returned for table '{table_name}'")

        except CatalogError:
            raise
        except Exception as e:
            raise CatalogError(f"Failed to fetch DDL for '{table_name}': {e}") from e

    def get_ddl_batch(self, table_names: List[str]) -> Dict[str, str]:
        """Fetch DDL for multiple tables from Databricks.

        Currently executes individual queries for each table.
        Future optimization could use parallel execution.

        Args:
            table_names: List of fully qualified table names.

        Returns:
            Dictionary mapping table names to their DDL statements.
            Tables that couldn't be found will have error messages as values
            prefixed with "ERROR: ".

        Raises:
            CatalogError: If the batch operation fails entirely.
        """
        results: Dict[str, str] = {}

        for table_name in table_names:
            try:
                results[table_name] = self.get_ddl(table_name)
            except CatalogError as e:
                # Store error message for this table but continue with others
                results[table_name] = f"ERROR: {e}"

        return results
