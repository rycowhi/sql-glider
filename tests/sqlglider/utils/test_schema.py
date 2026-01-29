"""Tests for schema utility functions."""

from sqlglider.utils.schema import parse_ddl_to_schema


class TestParseDdlToSchema:
    """Tests for parse_ddl_to_schema()."""

    def test_simple_create_table(self):
        ddl = "CREATE TABLE users (id BIGINT, name VARCHAR, email STRING)"
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        assert "users" in schema
        assert set(schema["users"].keys()) == {"id", "name", "email"}
        assert all(v == "UNKNOWN" for v in schema["users"].values())

    def test_qualified_table_name(self):
        ddl = "CREATE TABLE my_db.my_schema.users (id INT, name STRING)"
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        assert "my_db.my_schema.users" in schema

    def test_multiple_create_statements(self):
        ddl = """
        CREATE TABLE users (id INT, name STRING);
        CREATE TABLE orders (order_id INT, user_id INT, total DECIMAL);
        """
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        assert len(schema) == 2
        assert set(schema["users"].keys()) == {"id", "name"}
        assert set(schema["orders"].keys()) == {"order_id", "user_id", "total"}

    def test_create_view_as_select_not_extracted(self):
        """CREATE VIEW AS SELECT doesn't have ColumnDef nodes.

        Schema for views is handled by LineageAnalyzer._extract_schema_from_statement,
        not parse_ddl_to_schema (which only handles explicit DDL column definitions).
        """
        ddl = "CREATE VIEW user_view AS SELECT id, name FROM users"
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        assert schema == {}

    def test_non_create_statements_ignored(self):
        ddl = "SELECT * FROM users; INSERT INTO orders VALUES (1, 2, 3);"
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        assert schema == {}

    def test_empty_input(self):
        schema = parse_ddl_to_schema("", dialect="spark")
        assert schema == {}

    def test_create_without_columns(self):
        """CREATE TABLE AS SELECT has no column defs in the Schema node."""
        ddl = "CREATE TABLE target AS SELECT id, name FROM source"
        schema = parse_ddl_to_schema(ddl, dialect="spark")
        # CTAS doesn't have ColumnDef nodes — returns empty
        assert "target" not in schema
