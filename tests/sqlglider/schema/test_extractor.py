"""Tests for shared schema extraction logic."""

from pathlib import Path

import pytest
from rich.console import Console

from sqlglider.schema.extractor import (
    extract_and_resolve_schema,
    extract_schemas_from_files,
)


@pytest.fixture
def console():
    """Create a quiet console for testing."""
    return Console(stderr=True, quiet=True)


class TestExtractSchemasFromFiles:
    """Tests for extract_schemas_from_files."""

    def test_extract_from_create_view(self, tmp_path, console):
        """Test schema extraction from CREATE VIEW AS SELECT statements."""
        sql_file = tmp_path / "views.sql"
        sql_file.write_text("CREATE VIEW active_users AS SELECT id, name FROM users;")

        schema = extract_schemas_from_files(
            [sql_file], dialect="spark", console=console
        )

        assert "active_users" in schema
        assert "id" in schema["active_users"]
        assert "name" in schema["active_users"]

    def test_extract_from_create_table_as_select(self, tmp_path, console):
        """Test schema extraction from CREATE TABLE AS SELECT."""
        sql_file = tmp_path / "ctas.sql"
        sql_file.write_text(
            "CREATE TABLE customers AS SELECT id, name, email FROM raw_customers;"
        )

        schema = extract_schemas_from_files(
            [sql_file], dialect="spark", console=console
        )

        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]
        assert "email" in schema["customers"]

    def test_extract_from_dql_qualified_refs(self, tmp_path, console):
        """Test schema inference from qualified column references in DQL."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT c.id, c.name FROM customers c;")

        schema = extract_schemas_from_files(
            [sql_file], dialect="spark", console=console
        )

        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]

    def test_accumulates_across_files(self, tmp_path, console):
        """Test that schema accumulates across multiple files."""
        file1 = tmp_path / "a.sql"
        file1.write_text("CREATE VIEW customers AS SELECT id, name FROM raw_customers;")

        file2 = tmp_path / "b.sql"
        file2.write_text(
            "CREATE VIEW orders AS SELECT order_id, customer_id FROM raw_orders;"
        )

        schema = extract_schemas_from_files(
            [file1, file2], dialect="spark", console=console
        )

        assert "customers" in schema
        assert "orders" in schema
        assert "id" in schema["customers"]
        assert "order_id" in schema["orders"]

    def test_merges_columns_for_same_table(self, tmp_path, console):
        """Test that columns are merged when the same table appears in multiple files."""
        file1 = tmp_path / "a.sql"
        file1.write_text("SELECT c.id, c.name FROM customers c;")

        file2 = tmp_path / "b.sql"
        file2.write_text("SELECT c.id, c.age FROM customers c;")

        schema = extract_schemas_from_files(
            [file1, file2], dialect="spark", console=console
        )

        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]
        assert "age" in schema["customers"]

    def test_merges_columns_case_insensitive(self, tmp_path, console):
        """Test that tables with different casing are merged into one entry."""
        file1 = tmp_path / "a.sql"
        file1.write_text("SELECT c.id, c.name FROM Customers c;")

        file2 = tmp_path / "b.sql"
        file2.write_text("SELECT c.id, c.AGE FROM customers c;")

        schema = extract_schemas_from_files(
            [file1, file2], dialect="spark", console=console
        )

        assert "customers" in schema
        assert len([k for k in schema if k.lower() == "customers"]) == 1
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]
        assert "age" in schema["customers"]

    def test_initial_schema_normalized(self, tmp_path, console):
        """Test that initial schema keys are normalized to lowercase."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT o.id FROM orders o;")

        initial = {"Existing_Table": {"Col1": "UNKNOWN"}}
        schema = extract_schemas_from_files(
            [sql_file],
            dialect="spark",
            initial_schema=initial,
            console=console,
        )

        assert "existing_table" in schema
        assert "col1" in schema["existing_table"]
        assert "orders" in schema

    def test_initial_schema_preserved(self, tmp_path, console):
        """Test that initial schema is included in result."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT o.id FROM orders o;")

        initial = {"existing_table": {"col1": "UNKNOWN"}}
        schema = extract_schemas_from_files(
            [sql_file],
            dialect="spark",
            initial_schema=initial,
            console=console,
        )

        assert "existing_table" in schema
        assert "orders" in schema

    def test_nonfatal_parse_errors(self, tmp_path, console):
        """Test that parse errors in individual files are non-fatal."""
        good_file = tmp_path / "good.sql"
        good_file.write_text("SELECT u.id, u.name FROM users u;")

        bad_file = tmp_path / "bad.sql"
        bad_file.write_text("INVALID SQL ;;;")

        schema = extract_schemas_from_files(
            [bad_file, good_file], dialect="spark", console=console
        )

        assert "users" in schema

    def test_empty_file_list(self, console):
        """Test extraction with no files."""
        schema = extract_schemas_from_files([], dialect="spark", console=console)
        assert schema == {}

    def test_with_preprocessor(self, tmp_path, console):
        """Test that SQL preprocessor is applied."""
        sql_file = tmp_path / "template.sql"
        sql_file.write_text("CREATE VIEW PLACEHOLDER AS SELECT id FROM source;")

        def preprocessor(sql: str, path: Path) -> str:
            return sql.replace("PLACEHOLDER", "real_table")

        schema = extract_schemas_from_files(
            [sql_file],
            dialect="spark",
            sql_preprocessor=preprocessor,
            console=console,
        )

        assert "real_table" in schema
        assert "PLACEHOLDER" not in schema


class TestExtractAndResolveSchema:
    """Tests for extract_and_resolve_schema orchestrator."""

    def test_basic_extraction(self, tmp_path, console):
        """Test basic schema extraction without catalog."""
        sql_file = tmp_path / "schema.sql"
        sql_file.write_text("CREATE VIEW users AS SELECT id, name FROM raw_users;")

        schema = extract_and_resolve_schema(
            [sql_file], dialect="spark", console=console
        )

        assert "users" in schema
        assert "id" in schema["users"]

    def test_no_catalog_when_not_specified(self, tmp_path, console):
        """Test that catalog is not called when catalog_type is None."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")

        # Should not raise even without catalog dependencies
        schema = extract_and_resolve_schema(
            [sql_file], dialect="spark", console=console
        )

        assert isinstance(schema, dict)
