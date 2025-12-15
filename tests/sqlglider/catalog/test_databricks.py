"""Tests for Databricks catalog implementation."""

import os

import pytest
from pytest_mock import MockerFixture

from sqlglider.catalog.base import CatalogError
from sqlglider.catalog.databricks import DatabricksCatalog, _check_databricks_sdk


class TestDatabricksCatalogBasics:
    """Tests for DatabricksCatalog basic properties."""

    def test_name_property(self):
        """Test that name property returns 'databricks'."""
        catalog = DatabricksCatalog()
        assert catalog.name == "databricks"


class TestDatabricksCatalogConfigure:
    """Tests for DatabricksCatalog configure method."""

    def test_configure_with_warehouse_id(self, mocker: MockerFixture):
        """Test configuring with warehouse_id."""
        mocker.patch.dict(os.environ, {}, clear=True)

        catalog = DatabricksCatalog()
        catalog.configure({"warehouse_id": "test-warehouse-123"})

        assert catalog._warehouse_id == "test-warehouse-123"

    def test_configure_with_all_options(self, mocker: MockerFixture):
        """Test configuring with all options."""
        mocker.patch.dict(os.environ, {}, clear=True)

        catalog = DatabricksCatalog()
        catalog.configure(
            {
                "warehouse_id": "test-warehouse",
                "host": "https://test.databricks.com",
                "token": "dapi-test-token",
            }
        )

        assert catalog._warehouse_id == "test-warehouse"
        assert catalog._host == "https://test.databricks.com"
        assert catalog._token == "dapi-test-token"

    def test_configure_warehouse_from_env_var(self, mocker: MockerFixture):
        """Test configuring warehouse_id from environment variable."""
        mocker.patch.dict(
            os.environ,
            {
                "DATABRICKS_WAREHOUSE_ID": "env-warehouse",
            },
        )

        catalog = DatabricksCatalog()
        catalog.configure({})

        # Only warehouse_id is read from env var by configure()
        # Host/token are handled by the SDK's unified auth
        assert catalog._warehouse_id == "env-warehouse"
        assert catalog._host is None
        assert catalog._token is None

    def test_configure_cli_overrides_env(self, mocker: MockerFixture):
        """Test that config values override environment variables."""
        mocker.patch.dict(
            os.environ,
            {
                "DATABRICKS_WAREHOUSE_ID": "env-warehouse",
            },
        )

        catalog = DatabricksCatalog()
        catalog.configure({"warehouse_id": "config-warehouse"})

        assert catalog._warehouse_id == "config-warehouse"

    def test_configure_with_profile(self, mocker: MockerFixture):
        """Test configuring with Databricks CLI profile."""
        mocker.patch.dict(os.environ, {}, clear=True)

        catalog = DatabricksCatalog()
        catalog.configure({"warehouse_id": "test-warehouse", "profile": "dev-profile"})

        assert catalog._warehouse_id == "test-warehouse"
        assert catalog._profile == "dev-profile"

    def test_configure_raises_without_warehouse_id(self, mocker: MockerFixture):
        """Test that configure raises error without warehouse_id."""
        mocker.patch.dict(os.environ, {}, clear=True)

        catalog = DatabricksCatalog()
        with pytest.raises(CatalogError) as exc_info:
            catalog.configure({})

        assert "warehouse_id" in str(exc_info.value).lower()


class TestDatabricksCatalogGetDdl:
    """Tests for DatabricksCatalog get_ddl method."""

    @pytest.fixture
    def mock_workspace_client(self, mocker: MockerFixture):
        """Create a mock WorkspaceClient and patch the import."""
        mocker.patch("sqlglider.catalog.databricks._databricks_sdk_available", True)

        mock_client = mocker.MagicMock()

        # Patch the import inside _get_client
        mocker.patch(
            "databricks.sdk.WorkspaceClient", return_value=mock_client, create=True
        )

        return mock_client

    @pytest.fixture
    def configured_catalog(self, mock_workspace_client):
        """Create a configured catalog with mocked SDK."""
        catalog = DatabricksCatalog()
        catalog._warehouse_id = "test-warehouse"
        return catalog

    def test_get_ddl_raises_without_configuration(self):
        """Test that get_ddl raises error when not configured."""
        catalog = DatabricksCatalog()
        with pytest.raises(CatalogError) as exc_info:
            catalog.get_ddl("test.table")

        assert "not configured" in str(exc_info.value).lower()

    def test_get_ddl_success(self, configured_catalog, mock_workspace_client):
        """Test successful DDL retrieval."""
        mock_response = (
            mock_workspace_client.statement_execution.execute_statement.return_value
        )
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.result.data_array = [["CREATE TABLE test.schema.table (id INT)"]]

        ddl = configured_catalog.get_ddl("test.schema.table")

        assert "CREATE TABLE" in ddl
        mock_workspace_client.statement_execution.execute_statement.assert_called_once()

    def test_get_ddl_failed_query(self, configured_catalog, mock_workspace_client):
        """Test handling of failed query."""
        mock_response = (
            mock_workspace_client.statement_execution.execute_statement.return_value
        )
        mock_response.status.state.value = "FAILED"
        mock_response.status.error.message = "Table not found"

        with pytest.raises(CatalogError) as exc_info:
            configured_catalog.get_ddl("nonexistent.table")

        assert "Table not found" in str(exc_info.value)

    def test_get_ddl_empty_result(self, configured_catalog, mock_workspace_client):
        """Test handling of empty result."""
        mock_response = (
            mock_workspace_client.statement_execution.execute_statement.return_value
        )
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.result.data_array = []

        with pytest.raises(CatalogError) as exc_info:
            configured_catalog.get_ddl("empty.table")

        assert "no ddl" in str(exc_info.value).lower()


class TestDatabricksCatalogGetDdlBatch:
    """Tests for DatabricksCatalog get_ddl_batch method."""

    @pytest.fixture
    def mock_workspace_client(self, mocker: MockerFixture):
        """Create a mock WorkspaceClient and patch the import."""
        mocker.patch("sqlglider.catalog.databricks._databricks_sdk_available", True)

        mock_client = mocker.MagicMock()

        mocker.patch(
            "databricks.sdk.WorkspaceClient", return_value=mock_client, create=True
        )

        return mock_client

    def test_get_ddl_batch_returns_dict(self, mock_workspace_client):
        """Test that get_ddl_batch returns dictionary."""
        mock_response = (
            mock_workspace_client.statement_execution.execute_statement.return_value
        )
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.result.data_array = [["CREATE TABLE test (id INT)"]]

        catalog = DatabricksCatalog()
        catalog._warehouse_id = "test"

        result = catalog.get_ddl_batch(["table1", "table2"])

        assert isinstance(result, dict)
        assert len(result) == 2
        assert "table1" in result
        assert "table2" in result

    def test_get_ddl_batch_handles_errors(
        self, mock_workspace_client, mocker: MockerFixture
    ):
        """Test that get_ddl_batch handles individual table errors."""
        success_response = mocker.MagicMock()
        success_response.status.state.value = "SUCCEEDED"
        success_response.result.data_array = [["CREATE TABLE test (id INT)"]]

        fail_response = mocker.MagicMock()
        fail_response.status.state.value = "FAILED"
        fail_response.status.error.message = "Not found"

        mock_workspace_client.statement_execution.execute_statement.side_effect = [
            success_response,
            fail_response,
        ]

        catalog = DatabricksCatalog()
        catalog._warehouse_id = "test"

        result = catalog.get_ddl_batch(["table1", "table2"])

        assert "CREATE TABLE" in result["table1"]
        assert result["table2"].startswith("ERROR:")


class TestDatabricksSdkCheck:
    """Tests for SDK availability check."""

    def test_sdk_not_available_raises_error(self, mocker: MockerFixture):
        """Test that missing SDK raises helpful error."""
        import sqlglider.catalog.databricks as db_module

        db_module._databricks_sdk_available = False

        with pytest.raises(CatalogError) as exc_info:
            _check_databricks_sdk()

        assert "databricks-sdk" in str(exc_info.value)
        assert "pip install" in str(exc_info.value)
