"""Tests for lineage formatters."""

import csv
import json
from io import StringIO

from rich.console import Console

from sqlglider.global_models import AnalysisLevel
from sqlglider.lineage.analyzer import (
    LineageItem,
    ObjectType,
    QueryLineageResult,
    QueryMetadata,
    QueryTablesResult,
    TableInfo,
    TableUsage,
)
from sqlglider.lineage.formatters import (
    CsvFormatter,
    JsonFormatter,
    OutputWriter,
    TableCsvFormatter,
    TableJsonFormatter,
    TableTextFormatter,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.TABLE,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
            ),
            QueryLineageResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT b FROM t2"),
                lineage_items=[LineageItem(output_name="t2.b", source_name="t2.b")],
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.TABLE,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.COLUMN,
            ),
            QueryLineageResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT b"),
                lineage_items=[LineageItem(output_name="t2.b", source_name="t2.b")],
                level=AnalysisLevel.COLUMN,
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
                level=AnalysisLevel.TABLE,
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

    def test_write_to_file(self, tmp_path):
        """Test writing output to a file."""
        content = "test output"
        output_file = tmp_path / "output.txt"

        OutputWriter.write(content, output_file)
        assert output_file.read_text(encoding="utf-8") == content

    def test_write_to_stdout(self, capsys):
        """Test writing output to stdout."""
        content = "test output"
        OutputWriter.write(content)

        captured = capsys.readouterr()
        assert content in captured.out


def _capture_table_text_format(results):
    """Helper to capture TableTextFormatter output as a string."""
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False)
    TableTextFormatter.format(results, console)
    return buffer.getvalue()


class TestTableTextFormatter:
    """Tests for TableTextFormatter."""

    def test_format_single_query_single_table(self):
        """Test formatting a single query with one table."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers",
                ),
                tables=[
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            )
        ]

        result = _capture_table_text_format(results)

        assert "Query 0:" in result
        assert "Table Name" in result
        assert "Usage" in result
        assert "Type" in result
        assert "customers" in result
        assert "INPUT" in result
        assert "UNKNOWN" in result

    def test_format_multiple_tables(self):
        """Test formatting a query with multiple tables."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers JOIN orders",
                ),
                tables=[
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                    TableInfo(
                        name="orders",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = _capture_table_text_format(results)

        assert "customers" in result
        assert "orders" in result
        assert "Total: 2 table(s)" in result

    def test_format_create_view(self):
        """Test formatting CREATE VIEW with OUTPUT and VIEW type."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="CREATE VIEW customer_summary AS SELECT...",
                ),
                tables=[
                    TableInfo(
                        name="customer_summary",
                        usage=TableUsage.OUTPUT,
                        object_type=ObjectType.VIEW,
                    ),
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = _capture_table_text_format(results)

        assert "customer_summary" in result
        assert "OUTPUT" in result
        assert "VIEW" in result
        assert "INPUT" in result

    def test_format_cte(self):
        """Test formatting query with CTE."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="WITH cte AS (SELECT...) SELECT...",
                ),
                tables=[
                    TableInfo(
                        name="cte",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.CTE,
                    ),
                    TableInfo(
                        name="orders",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = _capture_table_text_format(results)

        assert "cte" in result
        assert "CTE" in result

    def test_format_multi_query(self):
        """Test formatting multiple queries."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT * FROM t1"),
                tables=[
                    TableInfo(
                        name="t1",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            ),
            QueryTablesResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT * FROM t2"),
                tables=[
                    TableInfo(
                        name="t2",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            ),
        ]

        result = _capture_table_text_format(results)

        assert "Query 0:" in result
        assert "Query 1:" in result
        assert "t1" in result
        assert "t2" in result

    def test_format_empty_results(self):
        """Test formatting empty results."""
        result = _capture_table_text_format([])
        assert "No tables found" in result

    def test_format_table_both_usage(self):
        """Test formatting table with BOTH usage."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="INSERT INTO t SELECT * FROM t",
                ),
                tables=[
                    TableInfo(
                        name="t",
                        usage=TableUsage.BOTH,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            )
        ]

        result = _capture_table_text_format(results)

        assert "BOTH" in result


class TestTableJsonFormatter:
    """Tests for TableJsonFormatter."""

    def test_format_single_query(self):
        """Test formatting a single query as JSON."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers",
                ),
                tables=[
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            )
        ]

        result = TableJsonFormatter.format(results)
        data = json.loads(result)

        assert "queries" in data
        assert len(data["queries"]) == 1
        assert data["queries"][0]["query_index"] == 0
        assert len(data["queries"][0]["tables"]) == 1
        assert data["queries"][0]["tables"][0]["name"] == "customers"
        assert data["queries"][0]["tables"][0]["usage"] == "INPUT"
        assert data["queries"][0]["tables"][0]["object_type"] == "UNKNOWN"

    def test_format_multi_query(self):
        """Test formatting multiple queries as JSON."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT * FROM t1"),
                tables=[
                    TableInfo(
                        name="t1",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            ),
            QueryTablesResult(
                metadata=QueryMetadata(query_index=1, query_preview="SELECT * FROM t2"),
                tables=[
                    TableInfo(
                        name="t2",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            ),
        ]

        result = TableJsonFormatter.format(results)
        data = json.loads(result)

        assert len(data["queries"]) == 2
        assert data["queries"][0]["query_index"] == 0
        assert data["queries"][1]["query_index"] == 1

    def test_format_create_table(self):
        """Test JSON formatting for CREATE TABLE."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="CREATE TABLE new_table AS SELECT...",
                ),
                tables=[
                    TableInfo(
                        name="new_table",
                        usage=TableUsage.OUTPUT,
                        object_type=ObjectType.TABLE,
                    ),
                    TableInfo(
                        name="source_table",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = TableJsonFormatter.format(results)
        data = json.loads(result)

        tables = data["queries"][0]["tables"]
        table_by_name = {t["name"]: t for t in tables}

        assert table_by_name["new_table"]["usage"] == "OUTPUT"
        assert table_by_name["new_table"]["object_type"] == "TABLE"
        assert table_by_name["source_table"]["usage"] == "INPUT"

    def test_format_all_object_types(self):
        """Test JSON formatting for all object types."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="Complex query"),
                tables=[
                    TableInfo(
                        name="t1", usage=TableUsage.INPUT, object_type=ObjectType.TABLE
                    ),
                    TableInfo(
                        name="v1", usage=TableUsage.INPUT, object_type=ObjectType.VIEW
                    ),
                    TableInfo(
                        name="c1", usage=TableUsage.INPUT, object_type=ObjectType.CTE
                    ),
                    TableInfo(
                        name="u1",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = TableJsonFormatter.format(results)
        data = json.loads(result)

        tables = data["queries"][0]["tables"]
        object_types = {t["name"]: t["object_type"] for t in tables}

        assert object_types["t1"] == "TABLE"
        assert object_types["v1"] == "VIEW"
        assert object_types["c1"] == "CTE"
        assert object_types["u1"] == "UNKNOWN"


class TestTableCsvFormatter:
    """Tests for TableCsvFormatter."""

    def test_format_single_table(self):
        """Test CSV formatting for single table."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(
                    query_index=0,
                    query_preview="SELECT * FROM customers",
                ),
                tables=[
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            )
        ]

        result = TableCsvFormatter.format(results)
        lines = [line.strip() for line in result.strip().split("\n")]

        assert lines[0] == "query_index,table_name,usage,object_type"
        assert lines[1] == "0,customers,INPUT,UNKNOWN"

    def test_format_multiple_tables(self):
        """Test CSV formatting with multiple tables."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT *"),
                tables=[
                    TableInfo(
                        name="customers",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                    TableInfo(
                        name="orders",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = TableCsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 3  # Header + 2 data rows
        assert rows[0] == ["query_index", "table_name", "usage", "object_type"]
        assert rows[1] == ["0", "customers", "INPUT", "UNKNOWN"]
        assert rows[2] == ["0", "orders", "INPUT", "UNKNOWN"]

    def test_format_multi_query(self):
        """Test CSV formatting with multiple queries."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="Query 1"),
                tables=[
                    TableInfo(
                        name="t1",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            ),
            QueryTablesResult(
                metadata=QueryMetadata(query_index=1, query_preview="Query 2"),
                tables=[
                    TableInfo(
                        name="t2", usage=TableUsage.OUTPUT, object_type=ObjectType.TABLE
                    )
                ],
            ),
        ]

        result = TableCsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 3  # Header + 2 data rows
        assert rows[1][0] == "0"  # First query index
        assert rows[2][0] == "1"  # Second query index

    def test_format_all_usage_types(self):
        """Test CSV formatting for all usage types."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="Complex"),
                tables=[
                    TableInfo(
                        name="input_only",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    ),
                    TableInfo(
                        name="output_only",
                        usage=TableUsage.OUTPUT,
                        object_type=ObjectType.TABLE,
                    ),
                    TableInfo(
                        name="both",
                        usage=TableUsage.BOTH,
                        object_type=ObjectType.UNKNOWN,
                    ),
                ],
            )
        ]

        result = TableCsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        usages = {row[1]: row[2] for row in rows[1:]}  # Skip header

        assert usages["input_only"] == "INPUT"
        assert usages["output_only"] == "OUTPUT"
        assert usages["both"] == "BOTH"

    def test_format_empty_results(self):
        """Test CSV formatting with empty results."""
        result = TableCsvFormatter.format([])
        assert result == ""

    def test_format_qualified_table_names(self):
        """Test CSV formatting with qualified table names."""
        results = [
            QueryTablesResult(
                metadata=QueryMetadata(query_index=0, query_preview="SELECT..."),
                tables=[
                    TableInfo(
                        name="schema.table",
                        usage=TableUsage.INPUT,
                        object_type=ObjectType.UNKNOWN,
                    )
                ],
            )
        ]

        result = TableCsvFormatter.format(results)
        reader = csv.reader(StringIO(result))
        rows = list(reader)

        assert rows[1][1] == "schema.table"
