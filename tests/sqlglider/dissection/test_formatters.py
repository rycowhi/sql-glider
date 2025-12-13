"""Tests for dissection formatters."""

import json

import pytest
from rich.console import Console

from sqlglider.dissection.formatters import (
    DissectionCsvFormatter,
    DissectionJsonFormatter,
    DissectionTextFormatter,
)
from sqlglider.dissection.models import (
    ComponentType,
    QueryDissectionResult,
    QueryMetadata,
    SQLComponent,
)


@pytest.fixture
def sample_results():
    """Create sample dissection results for testing."""
    return [
        QueryDissectionResult(
            metadata=QueryMetadata(
                query_index=0,
                query_preview="WITH cte AS (SELECT...",
                statement_type="INSERT",
                total_components=3,
            ),
            components=[
                SQLComponent(
                    component_type=ComponentType.CTE,
                    component_index=0,
                    name="order_totals",
                    sql="SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
                    location="WITH clause",
                    dependencies=[],
                ),
                SQLComponent(
                    component_type=ComponentType.TARGET_TABLE,
                    component_index=1,
                    name="analytics.summary",
                    sql="analytics.summary",
                    is_executable=False,
                    location="INSERT INTO target",
                ),
                SQLComponent(
                    component_type=ComponentType.SOURCE_QUERY,
                    component_index=2,
                    sql="SELECT * FROM order_totals WHERE total > 100",
                    dependencies=["order_totals"],
                    location="INSERT source SELECT",
                ),
            ],
            original_sql="WITH order_totals AS (...) INSERT INTO analytics.summary SELECT ...",
        )
    ]


class TestDissectionJsonFormatter:
    """Tests for JSON formatter."""

    def test_format_basic(self, sample_results):
        """Test basic JSON formatting."""
        output = DissectionJsonFormatter.format(sample_results)
        data = json.loads(output)

        assert "queries" in data
        assert len(data["queries"]) == 1

        query = data["queries"][0]
        assert query["query_index"] == 0
        assert query["statement_type"] == "INSERT"
        assert len(query["components"]) == 3

    def test_format_components(self, sample_results):
        """Test that components are properly formatted."""
        output = DissectionJsonFormatter.format(sample_results)
        data = json.loads(output)

        components = data["queries"][0]["components"]

        # Check CTE component
        cte = components[0]
        assert cte["component_type"] == "CTE"
        assert cte["name"] == "order_totals"
        assert cte["is_executable"] is True

        # Check TARGET_TABLE component
        target = components[1]
        assert target["component_type"] == "TARGET_TABLE"
        assert target["is_executable"] is False

    def test_format_dependencies(self, sample_results):
        """Test that dependencies are included."""
        output = DissectionJsonFormatter.format(sample_results)
        data = json.loads(output)

        source = data["queries"][0]["components"][2]
        assert source["dependencies"] == ["order_totals"]

    def test_format_original_sql(self, sample_results):
        """Test that original SQL is included."""
        output = DissectionJsonFormatter.format(sample_results)
        data = json.loads(output)

        assert "original_sql" in data["queries"][0]
        assert "INSERT" in data["queries"][0]["original_sql"]

    def test_format_empty_results(self):
        """Test formatting empty results."""
        output = DissectionJsonFormatter.format([])
        data = json.loads(output)
        assert data["queries"] == []


class TestDissectionCsvFormatter:
    """Tests for CSV formatter."""

    def test_format_basic(self, sample_results):
        """Test basic CSV formatting."""
        output = DissectionCsvFormatter.format(sample_results)
        lines = output.strip().split("\n")

        # Header + 3 data rows
        assert len(lines) == 4

        # Check header
        header = lines[0]
        assert "query_index" in header
        assert "component_type" in header
        assert "sql" in header

    def test_format_data_rows(self, sample_results):
        """Test that data rows are properly formatted."""
        output = DissectionCsvFormatter.format(sample_results)
        lines = output.strip().split("\n")

        # Skip header, check first data row (CTE)
        assert "CTE" in lines[1]
        assert "order_totals" in lines[1]

    def test_format_dependencies_semicolon_separated(self, sample_results):
        """Test that dependencies are semicolon-separated."""
        # Add a component with multiple dependencies
        sample_results[0].components.append(
            SQLComponent(
                component_type=ComponentType.MAIN_QUERY,
                component_index=3,
                sql="SELECT * FROM cte1 JOIN cte2",
                dependencies=["cte1", "cte2"],
                location="Top-level query",
            )
        )

        output = DissectionCsvFormatter.format(sample_results)
        assert "cte1;cte2" in output

    def test_format_empty_results(self):
        """Test formatting empty results."""
        output = DissectionCsvFormatter.format([])
        assert output == ""


class TestDissectionTextFormatter:
    """Tests for text formatter."""

    def test_format_basic(self, sample_results, capsys):
        """Test basic text formatting."""
        from io import StringIO

        buffer = StringIO()
        console = Console(file=buffer, force_terminal=False)

        DissectionTextFormatter.format(sample_results, console)

        output = buffer.getvalue()
        assert "Query 0" in output
        assert "INSERT" in output
        assert "Total components: 3" in output

    def test_format_empty_results(self, capsys):
        """Test formatting empty results."""
        from io import StringIO

        buffer = StringIO()
        console = Console(file=buffer, force_terminal=False)

        DissectionTextFormatter.format([], console)

        output = buffer.getvalue()
        assert "No dissection results found" in output
