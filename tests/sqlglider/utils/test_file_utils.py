"""Tests for file utility functions."""

import sys
from pathlib import Path

import pytest

from sqlglider.utils.file_utils import read_sql_file


class TestReadSqlFile:
    """Tests for read_sql_file function."""

    def test_read_valid_sql_file(self, tmp_path):
        """Test reading a valid SQL file."""
        sql_content = "SELECT * FROM customers WHERE id = 1;"

        sql_file = tmp_path / "query.sql"
        sql_file.write_text(sql_content)

        result = read_sql_file(sql_file)
        assert result == sql_content

    def test_read_multiline_sql_file(self, tmp_path):
        """Test reading a multiline SQL file."""
        sql_content = """SELECT
    customer_id,
    customer_name,
    total_orders
FROM customers
JOIN orders ON customers.id = orders.customer_id;"""

        sql_file = tmp_path / "query.sql"
        sql_file.write_text(sql_content)

        result = read_sql_file(sql_file)
        assert result == sql_content
        assert "SELECT" in result
        assert "JOIN" in result

    def test_read_empty_sql_file(self, tmp_path):
        """Test reading an empty SQL file."""
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")

        result = read_sql_file(sql_file)
        assert result == ""

    def test_read_sql_file_with_comments(self, tmp_path):
        """Test reading SQL file with comments."""
        sql_content = """-- This is a comment
SELECT * FROM customers;
/* Multi-line
   comment */
SELECT * FROM orders;"""

        sql_file = tmp_path / "query.sql"
        sql_file.write_text(sql_content)

        result = read_sql_file(sql_file)
        assert result == sql_content
        assert "-- This is a comment" in result
        assert "/* Multi-line" in result

    def test_read_sql_file_with_unicode(self, tmp_path):
        """Test reading SQL file with unicode characters."""
        sql_content = "SELECT '你好' AS greeting, 'café' AS word;"

        sql_file = tmp_path / "query.sql"
        sql_file.write_text(sql_content, encoding="utf-8")

        result = read_sql_file(sql_file)
        assert result == sql_content
        assert "你好" in result
        assert "café" in result

    def test_file_not_found(self):
        """Test reading a non-existent file raises FileNotFoundError."""
        non_existent_path = Path("/this/path/does/not/exist.sql")

        with pytest.raises(FileNotFoundError) as exc_info:
            read_sql_file(non_existent_path)

        assert "SQL file not found" in str(exc_info.value)
        assert str(non_existent_path) in str(exc_info.value)

    def test_path_is_directory(self, tmp_path):
        """Test that passing a directory raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            read_sql_file(tmp_path)

        assert "Path is not a file" in str(exc_info.value)
        assert str(tmp_path) in str(exc_info.value)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows file permissions work differently",
    )
    def test_permission_error(self, tmp_path):
        """Test handling of permission errors (Unix only)."""
        sql_file = tmp_path / "noperm.sql"
        sql_file.write_text("SELECT * FROM test;")

        # Change permissions to write-only (no read)
        sql_file.chmod(0o200)

        try:
            with pytest.raises(PermissionError) as exc_info:
                read_sql_file(sql_file)

            assert "Cannot read file" in str(exc_info.value)
        finally:
            # Restore permissions so pytest can clean up
            sql_file.chmod(0o600)

    def test_unicode_decode_error(self, tmp_path):
        """Test handling of invalid UTF-8 encoding."""
        sql_file = tmp_path / "invalid.sql"
        # Write invalid UTF-8 sequence
        sql_file.write_bytes(b"SELECT \xff\xfe FROM test;")

        with pytest.raises(UnicodeDecodeError) as exc_info:
            read_sql_file(sql_file)

        assert "not valid UTF-8" in str(exc_info.value)

    def test_read_large_sql_file(self, tmp_path):
        """Test reading a large SQL file."""
        # Generate a large SQL statement
        large_sql = "SELECT\n" + ",\n".join([f"    column_{i}" for i in range(1000)])
        large_sql += "\nFROM large_table;"

        sql_file = tmp_path / "large.sql"
        sql_file.write_text(large_sql)

        result = read_sql_file(sql_file)
        assert result == large_sql
        assert "column_1" in result
        assert "column_999" in result

    def test_read_sql_file_preserves_whitespace(self, tmp_path):
        """Test that reading preserves all whitespace."""
        sql_content = "SELECT\t\tcol1,\n    col2,\n        col3\nFROM\ttable;"

        sql_file = tmp_path / "whitespace.sql"
        sql_file.write_text(sql_content)

        result = read_sql_file(sql_file)
        assert result == sql_content
        # Verify tabs and multiple spaces are preserved
        assert "\t\t" in result
        assert "    " in result
