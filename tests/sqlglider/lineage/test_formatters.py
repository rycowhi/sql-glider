"""Tests for lineage formatters."""

import csv
import json
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

from rich.console import Console

from sqlglider.lineage.analyzer import (
    LineageItem,
    QueryLineageResult,
    QueryMetadata,
)
from sqlglider.lineage.formatters import (
    CsvFormatter,
    JsonFormatter,
    OutputWriter,
    TextFormatter,
)


def _capture_text_format(results):
    """Helper to capture TextFormatter output as a string."""
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False)
    TextFormatter.format(results, console)
    return buffer.getvalue()


class TestTextFormatter:
    """Tests for TextFormatter."""

    def test_format_single_query_single_column(self):
        """Test formatting a single query with one column."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT customer_name FROM customers",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="orders.customer_name",
                        source_name="customers.name",
                    )
                ],
                level="column",
            )
        ]

        result = _capture_text_format(results)

        assert "Query 0:" in result
        assert "Output Column" in result
        assert "Source Column" in result
        assert "orders.customer_name" in result
        assert "customers.name" in result

    def test_format_single_query_multiple_sources(self):
        """Test formatting a single query with multiple sources for one output."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT total FROM orders",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="orders.total",
                        source_name="order_items.price",
                    ),
                    LineageItem(
                        output_name="orders.total",
                        source_name="order_items.quantity",
                    ),
                ],
                level="column",
            )
        ]

        result = _capture_text_format(results)

        assert "orders.total" in result
        assert "order_items.price" in result
        assert "order_items.quantity" in result

    def test_format_multi_query(self):
        """Test formatting multiple queries."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT customer_id FROM orders",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="orders.customer_id",
                        source_name="customers.id",
                    )
                ],
                level="column",
            ),
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=1,
                    query_preview="SELECT product_id FROM products",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="products.product_id",
                        source_name="products.id",
                    )
                ],
                level="column",
            ),
        ]

        result = _capture_text_format(results)

        assert "Query 0:" in result
        assert "Query 1:" in result
        assert "orders.customer_id" in result
        assert "products.product_id" in result

    def test_format_table_lineage(self):
        """Test formatting table-level lineage."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers JOIN orders",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="query_result",
                        source_name="customers",
                    ),
                    LineageItem(
                        output_name="query_result",
                        source_name="orders",
                    ),
                ],
                level="table",
            )
        ]

        result = _capture_text_format(results)

        assert "Output Table" in result
        assert "Source Table" in result
        assert "query_result" in result
        assert "customers" in result
        assert "orders" in result

    def test_format_empty_results(self):
        """Test formatting empty results."""
        result = _capture_text_format([])
        assert "No lineage results found" in result

    def test_format_output_with_no_sources(self):
        """Test formatting output columns that have no sources."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT 'literal' as constant",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="constant",
                        source_name="",  # No source for literal
                    )
                ],
                level="column",
            )
        ]

        result = _capture_text_format(results)

        assert "constant" in result
        assert "(no sources)" in result

    def test_format_row_count(self):
        """Test that row count is displayed."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT a, b FROM t",
                ),
                lineage_items=[
                    LineageItem(output_name="t.a", source_name="t.a"),
                    LineageItem(output_name="t.b", source_name="t.b"),
                ],
                level="column",
            )
        ]

        result = _capture_text_format(results)

        assert "Total: 2 row(s)" in result


class TestJsonFormatter:
    """Tests for JsonFormatter."""

    def test_format_single_query(self):
        """Test formatting a single query as JSON."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT customer_name FROM customers",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="orders.customer_name",
                        source_name="customers.name",
                    )
                ],
                level="column",
            )
        ]

        result = JsonFormatter.format(results)
        data = json.loads(result)

        assert "queries" in data
        assert len(data["queries"]) == 1
        assert data["queries"][0]["query_index"] == 0
        assert data["queries"][0]["level"] == "column"
        assert len(data["queries"][0]["lineage"]) == 1
        assert data["queries"][0]["lineage"][0]["output_name"] == "orders.customer_name"
        assert data["queries"][0]["lineage"][0]["source_name"] == "customers.name"

    def test_format_multi_query(self):
        """Test formatting multiple queries as JSON."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT a FROM t1"),
                lineage_items=[LineageItem(output_name="t1.a", source_name="t1.a")],
                level="column",
            ),
            QueryLineageResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT b FROM t2"),
                lineage_items=[LineageItem(output_name="t2.b", source_name="t2.b")],
                level="column",
            ),
        ]

        result = JsonFormatter.format(results)
        data = json.loads(result)

        assert len(data["queries"]) == 2
        assert data["queries"][0]["query_index"] == 0
        assert data["queries"][1]["query_index"] == 1

    def test_format_table_lineage(self):
        """Test formatting table-level lineage as JSON."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="query_result",
                        source_name="customers",
                    )
                ],
                level="table",
            )
        ]

        result = JsonFormatter.format(results)
        data = json.loads(result)

        assert data["queries"][0]["level"] == "table"
        assert data["queries"][0]["lineage"][0]["output_name"] == "query_result"
        assert data["queries"][0]["lineage"][0]["source_name"] == "customers"


class TestCsvFormatter:
    """Tests for CsvFormatter."""

    def test_format_column_lineage(self):
        """Test formatting column lineage as CSV."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT customer_name FROM customers",
                ),
                lineage_items=[
                    LineageItem(
                        output_name="orders.customer_name",
                        source_name="customers.name",
                    )
                ],
                level="column",
            )
        ]

        result = CsvFormatter.format(results)
        lines = [line.strip() for line in result.strip().split("\n")]

        assert lines[0] == "query_index,output_column,source_column"
        assert lines[1] == "0,orders.customer_name,customers.name"

    def test_format_multiple_sources(self):
        """Test CSV formatting with multiple sources per output."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT total"),
                lineage_items=[
                    LineageItem(
                        output_name="orders.total",
                        source_name="order_items.price",
                    ),
                    LineageItem(
                        output_name="orders.total",
                        source_name="order_items.quantity",
                    ),
                ],
                level="column",
            )
        ]

        result = CsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 3  # Header + 2 data rows
        assert rows[0] == ["query_index", "output_column", "source_column"]
        assert rows[1] == ["0", "orders.total", "order_items.price"]
        assert rows[2] == ["0", "orders.total", "order_items.quantity"]

    def test_format_multi_query(self):
        """Test CSV formatting with multiple queries."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT a"),
                lineage_items=[LineageItem(output_name="t1.a", source_name="t1.a")],
                level="column",
            ),
            QueryLineageResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT b"),
                lineage_items=[LineageItem(output_name="t2.b", source_name="t2.b")],
                level="column",
            ),
        ]

        result = CsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 3  # Header + 2 data rows
        assert rows[1][0] == "0"  # First query
        assert rows[2][0] == "1"  # Second query

    def test_format_table_lineage(self):
        """Test CSV formatting for table-level lineage."""
        results = [
            QueryLineageResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT *"),
                lineage_items=[
                    LineageItem(output_name="query_result", source_name="customers"),
                    LineageItem(output_name="query_result", source_name="orders"),
                ],
                level="table",
            )
        ]

        result = CsvFormatter.format(results)
        lines = [line.strip() for line in result.strip().split("\n")]

        assert lines[0] == "query_index,output_table,source_table"
        assert "0,query_result,customers" in lines
        assert "0,query_result,orders" in lines

    def test_format_empty_results(self):
        """Test CSV formatting with empty results."""
        result = CsvFormatter.format([])
        assert result == ""


class TestOutputWriter:
    """Tests for OutputWriter."""

    def test_write_to_file(self):
        """Test writing output to a file."""
        content = "test output"

        with NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            temp_path = Path(f.name)

        try:
            OutputWriter.write(content, temp_path)
            assert temp_path.read_text(encoding="utf-8") == content
        finally:
            temp_path.unlink()

    def test_write_to_stdout(self, capsys):
        """Test writing output to stdout."""
        content = "test output"
        OutputWriter.write(content)

        captured = capsys.readouterr()
        assert content in captured.out
