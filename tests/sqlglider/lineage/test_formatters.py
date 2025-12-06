"""Tests for lineage formatters."""

import csv
import json
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from sqlglider.lineage.analyzer import ColumnLineage, QueryLineage, TableLineage
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


class TestMultiQueryFormatters:
    """Tests for multi-query formatter methods."""

    @pytest.fixture
    def sample_query_lineage(self):
        """Sample QueryLineage objects for testing."""
        return [
            QueryLineage(
                query_index=0,
                query_preview="SELECT customer_id, customer_name FROM customers",
                column_lineage=[
                    ColumnLineage(
                        output_column="customers.customer_id",
                        source_columns=["customers.customer_id"]
                    ),
                    ColumnLineage(
                        output_column="customers.customer_name",
                        source_columns=["customers.customer_name"]
                    ),
                ],
            ),
            QueryLineage(
                query_index=1,
                query_preview="SELECT order_id, customer_id FROM orders",
                column_lineage=[
                    ColumnLineage(
                        output_column="orders.order_id",
                        source_columns=["orders.order_id"]
                    ),
                    ColumnLineage(
                        output_column="orders.customer_id",
                        source_columns=["orders.customer_id"]
                    ),
                ],
            ),
        ]

    def test_text_formatter_multi_query(self, sample_query_lineage):
        """Test text formatter with multi-query results."""
        formatted = TextFormatter.format_multi_query(sample_query_lineage)

        # Check query headers
        assert "Query 0:" in formatted
        assert "Query 1:" in formatted
        assert "==========" in formatted  # Query separator

        # Check query previews
        assert "SELECT customer_id, customer_name FROM customers" in formatted
        assert "SELECT order_id, customer_id FROM orders" in formatted

        # Check column lineage
        assert "customers.customer_id" in formatted
        assert "orders.order_id" in formatted

        # Check that separators are present
        assert "----------" in formatted  # Column separator

    def test_text_formatter_multi_query_empty_list(self):
        """Test text formatter with empty multi-query list."""
        formatted = TextFormatter.format_multi_query([])
        assert formatted == ""

    def test_json_formatter_multi_query(self, sample_query_lineage):
        """Test JSON formatter with multi-query results."""
        formatted = JsonFormatter.format_multi_query(sample_query_lineage)
        data = json.loads(formatted)

        # Check structure
        assert "queries" in data
        assert len(data["queries"]) == 2

        # Check query 0
        assert data["queries"][0]["query_index"] == 0
        assert data["queries"][0]["query_preview"] == "SELECT customer_id, customer_name FROM customers"
        assert len(data["queries"][0]["column_lineage"]) == 2
        assert data["queries"][0]["column_lineage"][0]["output_column"] == "customers.customer_id"

        # Check query 1
        assert data["queries"][1]["query_index"] == 1
        assert data["queries"][1]["query_preview"] == "SELECT order_id, customer_id FROM orders"
        assert len(data["queries"][1]["column_lineage"]) == 2

    def test_json_formatter_multi_query_empty_list(self):
        """Test JSON formatter with empty multi-query list."""
        formatted = JsonFormatter.format_multi_query([])
        data = json.loads(formatted)

        assert "queries" in data
        assert data["queries"] == []

    def test_csv_formatter_multi_query(self, sample_query_lineage):
        """Test CSV formatter with multi-query results."""
        formatted = CsvFormatter.format_multi_query(sample_query_lineage)

        lines = formatted.strip().split("\n")
        assert len(lines) >= 5  # Header + 4 data rows (2 columns per query)

        # Check header
        assert "query_index" in lines[0]
        assert "query_preview" in lines[0]
        assert "output_column" in lines[0]
        assert "source_table" in lines[0]
        assert "source_column" in lines[0]

        # Parse CSV to verify structure
        reader = csv.DictReader(StringIO(formatted))
        rows = list(reader)

        assert len(rows) == 4  # 2 columns * 2 queries

        # Check first row (query 0, first column)
        assert rows[0]["query_index"] == "0"
        assert "customers" in rows[0]["query_preview"]
        assert rows[0]["output_column"] == "customers.customer_id"
        assert rows[0]["source_table"] == "customers"
        assert rows[0]["source_column"] == "customer_id"

        # Check third row (query 1, first column)
        assert rows[2]["query_index"] == "1"
        assert "orders" in rows[2]["query_preview"]
        assert rows[2]["output_column"] == "orders.order_id"

    def test_csv_formatter_multi_query_empty_list(self):
        """Test CSV formatter with empty multi-query list."""
        formatted = CsvFormatter.format_multi_query([])

        # Should only have header
        lines = formatted.strip().split("\n")
        assert len(lines) == 1
        assert "query_index,query_preview,output_column,source_table,source_column" in formatted

    def test_multi_query_with_complex_lineage(self):
        """Test multi-query formatting with complex lineage (multiple sources)."""
        query_lineage = [
            QueryLineage(
                query_index=0,
                query_preview="SELECT c.name, SUM(o.amount) FROM customers c JOIN orders o...",
                column_lineage=[
                    ColumnLineage(
                        output_column="aggregated.customer_name",
                        source_columns=["customers.name"]
                    ),
                    ColumnLineage(
                        output_column="aggregated.total_amount",
                        source_columns=["orders.amount"]  # Aggregated
                    ),
                ],
            ),
        ]

        # Test all formatters can handle it
        text_output = TextFormatter.format_multi_query(query_lineage)
        assert "aggregated.customer_name" in text_output
        assert "customers.name" in text_output

        json_output = JsonFormatter.format_multi_query(query_lineage)
        data = json.loads(json_output)
        assert len(data["queries"]) == 1
        assert len(data["queries"][0]["column_lineage"]) == 2

        csv_output = CsvFormatter.format_multi_query(query_lineage)
        assert "aggregated.customer_name" in csv_output
        assert "customers,name" in csv_output


class TestMultiQueryTableFormatters:
    """Tests for multi-query table lineage formatters."""

    @pytest.fixture
    def sample_query_table_lineage(self):
        """Sample QueryTableLineage objects for testing."""
        from sqlglider.lineage.analyzer import QueryTableLineage, TableLineage

        return [
            QueryTableLineage(
                query_index=0,
                query_preview="SELECT * FROM customers",
                table_lineage=TableLineage(
                    output_table="query_result",
                    source_tables=["customers"]
                )
            ),
            QueryTableLineage(
                query_index=1,
                query_preview="SELECT * FROM orders JOIN products ...",
                table_lineage=TableLineage(
                    output_table="query_result",
                    source_tables=["orders", "products"]
                )
            ),
        ]

    def test_text_formatter_multi_query_table(self, sample_query_table_lineage):
        """Test text formatter with multi-query table lineage."""
        formatted = TextFormatter.format_multi_query_table(sample_query_table_lineage)

        assert "Query 0:" in formatted
        assert "Query 1:" in formatted
        assert "customers" in formatted
        assert "orders" in formatted
        assert "products" in formatted

    def test_json_formatter_multi_query_table(self, sample_query_table_lineage):
        """Test JSON formatter with multi-query table lineage."""
        import json

        formatted = JsonFormatter.format_multi_query_table(sample_query_table_lineage)
        data = json.loads(formatted)

        assert "queries" in data
        assert len(data["queries"]) == 2
        assert data["queries"][0]["query_index"] == 0
        assert len(data["queries"][1]["table_lineage"]["source_tables"]) == 2

    def test_csv_formatter_multi_query_table(self, sample_query_table_lineage):
        """Test CSV formatter with multi-query table lineage."""
        formatted = CsvFormatter.format_multi_query_table(sample_query_table_lineage)

        lines = formatted.strip().split("\n")
        assert len(lines) >= 4  # Header + 3 rows (1 for query 0, 2 for query 1)

        # Check header
        assert "query_index" in lines[0]
        assert "output_table" in lines[0]
        assert "source_table" in lines[0]

        # Parse CSV
        reader = csv.DictReader(StringIO(formatted))
        rows = list(reader)

        assert len(rows) == 3  # 1 source table for query 0, 2 for query 1
        assert rows[0]["query_index"] == "0"
        assert rows[0]["source_table"] == "customers"
        assert rows[1]["query_index"] == "1"
        assert rows[2]["query_index"] == "1"
