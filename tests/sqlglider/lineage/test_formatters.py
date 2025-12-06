"""Tests for lineage formatters."""

import csv
import json
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from sqlglider.lineage.analyzer import ColumnLineage, TableLineage
from sqlglider.lineage.formatters import (
    CsvFormatter,
    JsonFormatter,
    OutputWriter,
    TextFormatter,
)


class TestTextFormatter:
    """Tests for TextFormatter."""

    def test_format_single_column(self):
        """Test formatting a single column lineage."""
        lineage = [
            ColumnLineage(
                output_column="orders.customer_name",
                source_columns=["customers.name"],
            )
        ]

        result = TextFormatter.format(lineage)

        assert "----------" in result
        assert "orders.customer_name" in result
        assert "customers.name" in result

    def test_format_multiple_columns(self):
        """Test formatting multiple column lineages."""
        lineage = [
            ColumnLineage(
                output_column="orders.customer_id",
                source_columns=["customers.id"],
            ),
            ColumnLineage(
                output_column="orders.total",
                source_columns=["order_items.price", "order_items.quantity"],
            ),
        ]

        result = TextFormatter.format(lineage)

        assert "orders.customer_id" in result
        assert "customers.id" in result
        assert "orders.total" in result
        assert "order_items.price" in result
        assert "order_items.quantity" in result

    def test_format_empty_list(self):
        """Test formatting an empty lineage list."""
        lineage = []
        result = TextFormatter.format(lineage)
        assert result == ""

    def test_format_table(self):
        """Test formatting table lineage."""
        lineage = TableLineage(
            output_table="orders",
            source_tables=["customers", "order_items", "products"],
        )

        result = TextFormatter.format_table(lineage)

        assert "----------" in result
        assert "orders" in result
        assert "customers" in result
        assert "order_items" in result
        assert "products" in result

    def test_format_table_single_source(self):
        """Test formatting table lineage with single source."""
        lineage = TableLineage(
            output_table="view_customers",
            source_tables=["customers"],
        )

        result = TextFormatter.format_table(lineage)

        assert "view_customers" in result
        assert "customers" in result


class TestJsonFormatter:
    """Tests for JsonFormatter."""

    def test_format_single_column(self):
        """Test formatting a single column lineage as JSON."""
        lineage = [
            ColumnLineage(
                output_column="orders.customer_name",
                source_columns=["customers.name"],
            )
        ]

        result = JsonFormatter.format(lineage)
        parsed = json.loads(result)

        assert "columns" in parsed
        assert len(parsed["columns"]) == 1
        assert parsed["columns"][0]["output_column"] == "orders.customer_name"
        assert parsed["columns"][0]["source_columns"] == ["customers.name"]

    def test_format_multiple_columns(self):
        """Test formatting multiple column lineages as JSON."""
        lineage = [
            ColumnLineage(
                output_column="orders.customer_id",
                source_columns=["customers.id"],
            ),
            ColumnLineage(
                output_column="orders.total",
                source_columns=["order_items.price", "order_items.quantity"],
            ),
        ]

        result = JsonFormatter.format(lineage)
        parsed = json.loads(result)

        assert len(parsed["columns"]) == 2
        assert parsed["columns"][0]["output_column"] == "orders.customer_id"
        assert parsed["columns"][1]["output_column"] == "orders.total"
        assert len(parsed["columns"][1]["source_columns"]) == 2

    def test_format_empty_list(self):
        """Test formatting an empty lineage list as JSON."""
        lineage = []
        result = JsonFormatter.format(lineage)
        parsed = json.loads(result)

        assert parsed == {"columns": []}

    def test_format_table(self):
        """Test formatting table lineage as JSON."""
        lineage = TableLineage(
            output_table="orders",
            source_tables=["customers", "order_items"],
        )

        result = JsonFormatter.format_table(lineage)
        parsed = json.loads(result)

        assert "table" in parsed
        assert parsed["table"]["output_table"] == "orders"
        assert parsed["table"]["source_tables"] == ["customers", "order_items"]

    def test_json_is_properly_indented(self):
        """Test that JSON output is properly indented."""
        lineage = [
            ColumnLineage(
                output_column="test.col",
                source_columns=["source.col"],
            )
        ]

        result = JsonFormatter.format(lineage)

        # Check for indentation (2 spaces)
        assert "  " in result
        # Check for newlines
        assert "\n" in result


class TestCsvFormatter:
    """Tests for CsvFormatter."""

    def test_format_single_column(self):
        """Test formatting a single column lineage as CSV."""
        lineage = [
            ColumnLineage(
                output_column="orders.customer_name",
                source_columns=["customers.name"],
            )
        ]

        result = CsvFormatter.format(lineage)
        lines = result.strip().splitlines()

        assert len(lines) == 2  # Header + 1 data row
        assert lines[0] == "output_column,source_table,source_column"
        assert "orders.customer_name,customers,name" in lines[1]

    def test_format_multiple_sources(self):
        """Test formatting column with multiple sources as CSV."""
        lineage = [
            ColumnLineage(
                output_column="orders.total",
                source_columns=["order_items.price", "order_items.quantity"],
            )
        ]

        result = CsvFormatter.format(lineage)
        lines = result.strip().split("\n")

        assert len(lines) == 3  # Header + 2 data rows
        assert "orders.total,order_items,price" in result
        assert "orders.total,order_items,quantity" in result

    def test_format_column_without_table(self):
        """Test formatting column without table prefix."""
        lineage = [
            ColumnLineage(
                output_column="customer_name",
                source_columns=["name"],
            )
        ]

        result = CsvFormatter.format(lineage)
        lines = result.strip().split("\n")

        # Should have empty source_table when no table prefix
        assert "customer_name,,name" in result

    def test_format_empty_list(self):
        """Test formatting an empty lineage list as CSV."""
        lineage = []
        result = CsvFormatter.format(lineage)

        # Should only have header
        assert result.strip() == "output_column,source_table,source_column"

    def test_format_table(self):
        """Test formatting table lineage as CSV."""
        lineage = TableLineage(
            output_table="orders",
            source_tables=["customers", "order_items", "products"],
        )

        result = CsvFormatter.format_table(lineage)
        lines = result.strip().splitlines()

        assert len(lines) == 4  # Header + 3 data rows
        assert lines[0] == "output_table,source_table"
        assert "orders,customers" in result
        assert "orders,order_items" in result
        assert "orders,products" in result

    def test_csv_parsing(self):
        """Test that CSV output can be parsed by csv module."""
        lineage = [
            ColumnLineage(
                output_column="orders.total",
                source_columns=["items.price", "items.qty"],
            )
        ]

        result = CsvFormatter.format(lineage)
        reader = csv.DictReader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["output_column"] == "orders.total"
        assert rows[0]["source_table"] == "items"
        assert rows[0]["source_column"] == "price"


class TestOutputWriter:
    """Tests for OutputWriter."""

    def test_write_to_stdout(self, capsys):
        """Test writing to stdout."""
        content = "test output content"
        OutputWriter.write(content, None)

        captured = capsys.readouterr()
        assert captured.out.strip() == content

    def test_write_to_file(self):
        """Test writing to a file."""
        content = "test file content\nline 2\nline 3"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            temp_path = Path(f.name)

        try:
            OutputWriter.write(content, temp_path)

            # Verify file was written
            assert temp_path.exists()
            assert temp_path.read_text(encoding="utf-8") == content
        finally:
            temp_path.unlink()

    def test_write_unicode_to_file(self):
        """Test writing unicode content to file."""
        content = "Unicode content: 你好 🚀 café"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            temp_path = Path(f.name)

        try:
            OutputWriter.write(content, temp_path)
            assert temp_path.read_text(encoding="utf-8") == content
        finally:
            temp_path.unlink()

    def test_write_overwrites_existing_file(self):
        """Test that writing overwrites existing file content."""
        with NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            temp_path = Path(f.name)
            f.write("old content")

        try:
            new_content = "new content"
            OutputWriter.write(new_content, temp_path)
            assert temp_path.read_text(encoding="utf-8") == new_content
        finally:
            temp_path.unlink()
