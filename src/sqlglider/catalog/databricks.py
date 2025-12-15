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
        Authentication is handled by the Databricks SDK's unified authentication,
        which automatically tries multiple methods in order:

        1. Direct configuration (host + token in sqlglider.toml)
        2. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
        3. Databricks CLI profile (~/.databrickscfg) - use 'profile' config option
        4. Azure CLI authentication (for Azure Databricks)
        5. Google Cloud authentication (for GCP Databricks)
        6. OAuth M2M (client credentials) via environment variables:
           - DATABRICKS_CLIENT_ID
           - DATABRICKS_CLIENT_SECRET

        For OAuth M2M, set these environment variables:
            export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
            export DATABRICKS_CLIENT_ID=your-client-id
            export DATABRICKS_CLIENT_SECRET=your-client-secret

        For Databricks CLI profile, either:
            - Configure DEFAULT profile in ~/.databrickscfg
            - Set profile name in sqlglider.toml: profile = "my-profile"

    Configuration:
        - warehouse_id (required): SQL warehouse ID for statement execution
        - profile (optional): Databricks CLI profile name from ~/.databrickscfg
        - host (optional): Databricks workspace URL
        - token (optional): Personal access token (legacy, prefer OAuth)

    Example:
        >>> # Using environment variables or CLI profile (recommended)
        >>> catalog = DatabricksCatalog()
        >>> catalog.configure({"warehouse_id": "abc123def456"})
        >>> ddl = catalog.get_ddl("my_catalog.my_schema.my_table")

        >>> # Using specific CLI profile
        >>> catalog.configure({"warehouse_id": "abc123", "profile": "dev-workspace"})
    """

    def __init__(self) -> None:
        """Initialize the Databricks catalog."""
        self._warehouse_id: Optional[str] = None
        self._profile: Optional[str] = None
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
                - profile: Databricks CLI profile name from ~/.databrickscfg
                - host: Databricks workspace URL (only needed if not using profile/env)
                - token: Personal access token (legacy, prefer OAuth or profile)

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

        # Get optional profile for CLI profile-based auth
        self._profile = config.get("profile")

        # Get optional host and token - only from config, not env vars
        # Let the SDK handle env var discovery for better unified auth support
        self._host = config.get("host")
        self._token = config.get("token")

        # Reset client so it gets recreated with new config
        self._client = None

    def _get_client(self) -> Any:
        """Get or create the Databricks WorkspaceClient.

        The SDK uses unified authentication, trying methods in this order:
        1. Explicit host/token if provided in config
        2. Profile from ~/.databrickscfg if specified
        3. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
        4. OAuth M2M via DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET
        5. Azure CLI / Google Cloud auth for cloud-hosted workspaces

        Returns:
            The WorkspaceClient instance.

        Raises:
            CatalogError: If the SDK is not installed or authentication fails.
        """
        _check_databricks_sdk()

        if self._client is None:
            from databricks.sdk import WorkspaceClient

            # Build kwargs for WorkspaceClient
            # Only pass values that are explicitly configured
            # Let SDK handle env var discovery for unified auth
            kwargs: Dict[str, Any] = {}
            if self._profile:
                kwargs["profile"] = self._profile
            if self._host:
                kwargs["host"] = self._host
            if self._token:
                kwargs["token"] = self._token

            try:
                self._client = WorkspaceClient(**kwargs)
            except Exception as e:
                raise CatalogError(
                    f"Failed to authenticate with Databricks: {e}"
                ) from e

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
