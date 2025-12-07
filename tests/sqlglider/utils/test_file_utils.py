"""Tests for file utility functions."""

import sys
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from sqlglider.utils.file_utils import read_sql_file


class TestReadSqlFile:
    """Tests for read_sql_file function."""

    def test_read_valid_sql_file(self):
        """Test reading a valid SQL file."""
        sql_content = "SELECT * FROM customers WHERE id = 1;"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == sql_content
        finally:
            temp_path.unlink()

    def test_read_multiline_sql_file(self):
        """Test reading a multiline SQL file."""
        sql_content = """SELECT
    customer_id,
    customer_name,
    total_orders
FROM customers
JOIN orders ON customers.id = orders.customer_id;"""

        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == sql_content
            assert "SELECT" in result
            assert "JOIN" in result
        finally:
            temp_path.unlink()

    def test_read_empty_sql_file(self):
        """Test reading an empty SQL file."""
        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == ""
        finally:
            temp_path.unlink()

    def test_read_sql_file_with_comments(self):
        """Test reading SQL file with comments."""
        sql_content = """-- This is a comment
SELECT * FROM customers;
/* Multi-line
   comment */
SELECT * FROM orders;"""

        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == sql_content
            assert "-- This is a comment" in result
            assert "/* Multi-line" in result
        finally:
            temp_path.unlink()

    def test_read_sql_file_with_unicode(self):
        """Test reading SQL file with unicode characters."""
        sql_content = "SELECT '你好' AS greeting, 'café' AS word;"

        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == sql_content
            assert "你好" in result
            assert "café" in result
        finally:
            temp_path.unlink()

    def test_file_not_found(self):
        """Test reading a non-existent file raises FileNotFoundError."""
        non_existent_path = Path("/this/path/does/not/exist.sql")

        with pytest.raises(FileNotFoundError) as exc_info:
            read_sql_file(non_existent_path)

        assert "SQL file not found" in str(exc_info.value)
        assert str(non_existent_path) in str(exc_info.value)

    def test_path_is_directory(self):
        """Test that passing a directory raises ValueError."""
        with TemporaryDirectory() as temp_dir:
            dir_path = Path(temp_dir)

            with pytest.raises(ValueError) as exc_info:
                read_sql_file(dir_path)

            assert "Path is not a file" in str(exc_info.value)
            assert str(dir_path) in str(exc_info.value)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows file permissions work differently",
    )
    def test_permission_error(self):
        """Test handling of permission errors (Unix only)."""
        # Create a file and make it unreadable
        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write("SELECT * FROM test;")
            temp_path = Path(f.name)

        try:
            # Change permissions to write-only (no read)
            temp_path.chmod(0o200)

            with pytest.raises(PermissionError) as exc_info:
                read_sql_file(temp_path)

            assert "Cannot read file" in str(exc_info.value)
        finally:
            # Restore permissions to delete file
            temp_path.chmod(0o600)
            temp_path.unlink()

    def test_unicode_decode_error(self):
        """Test handling of invalid UTF-8 encoding."""
        # Create a file with invalid UTF-8 bytes
        with NamedTemporaryFile(mode="wb", delete=False, suffix=".sql") as f:
            # Write invalid UTF-8 sequence
            f.write(b"SELECT \xff\xfe FROM test;")
            temp_path = Path(f.name)

        try:
            with pytest.raises(UnicodeDecodeError) as exc_info:
                read_sql_file(temp_path)

            assert "not valid UTF-8" in str(exc_info.value)
        finally:
            temp_path.unlink()

    def test_read_large_sql_file(self):
        """Test reading a large SQL file."""
        # Generate a large SQL statement
        large_sql = "SELECT\n" + ",\n".join([f"    column_{i}" for i in range(1000)])
        large_sql += "\nFROM large_table;"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write(large_sql)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == large_sql
            assert "column_1" in result
            assert "column_999" in result
        finally:
            temp_path.unlink()

    def test_read_sql_file_preserves_whitespace(self):
        """Test that reading preserves all whitespace."""
        sql_content = "SELECT\t\tcol1,\n    col2,\n        col3\nFROM\ttable;"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        try:
            result = read_sql_file(temp_path)
            assert result == sql_content
            # Verify tabs and multiple spaces are preserved
            assert "\t\t" in result
            assert "    " in result
        finally:
            temp_path.unlink()
