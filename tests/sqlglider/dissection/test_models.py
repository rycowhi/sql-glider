"""Tests for dissection models."""

import pytest

from sqlglider.dissection.models import (
    ComponentType,
    QueryDissectionResult,
    QueryMetadata,
    SQLComponent,
)


class TestQueryDissectionResult:
    """Tests for QueryDissectionResult model."""

    @pytest.fixture
    def sample_result(self):
        """Create a sample dissection result."""
        return QueryDissectionResult(
            metadata=QueryMetadata(
                query_index=0,
                query_preview="WITH cte AS (...",
                statement_type="INSERT",
                total_components=3,
            ),
            components=[
                SQLComponent(
                    component_type=ComponentType.CTE,
                    component_index=0,
                    name="order_totals",
                    sql="SELECT id FROM orders",
                    location="WITH clause",
                ),
                SQLComponent(
                    component_type=ComponentType.TARGET_TABLE,
                    component_index=1,
                    name="target_table",
                    sql="target_table",
                    is_executable=False,
                    location="INSERT INTO target",
                ),
                SQLComponent(
                    component_type=ComponentType.SOURCE_QUERY,
                    component_index=2,
                    sql="SELECT * FROM order_totals",
                    dependencies=["order_totals"],
                    location="INSERT source SELECT",
                ),
            ],
            original_sql="WITH order_totals AS (...) INSERT INTO target_table SELECT ...",
        )

    def test_get_component_by_name(self, sample_result):
        """Test finding component by name."""
        component = sample_result.get_component_by_name("order_totals")
        assert component is not None
        assert component.component_type == ComponentType.CTE

    def test_get_component_by_name_case_insensitive(self, sample_result):
        """Test case-insensitive component lookup."""
        component = sample_result.get_component_by_name("ORDER_TOTALS")
        assert component is not None
        assert component.name == "order_totals"

    def test_get_component_by_name_not_found(self, sample_result):
        """Test component lookup when not found."""
        component = sample_result.get_component_by_name("nonexistent")
        assert component is None

    def test_get_components_by_type(self, sample_result):
        """Test filtering components by type."""
        ctes = sample_result.get_components_by_type(ComponentType.CTE)
        assert len(ctes) == 1
        assert ctes[0].name == "order_totals"

    def test_get_components_by_type_empty(self, sample_result):
        """Test filtering when no components match."""
        subqueries = sample_result.get_components_by_type(ComponentType.SUBQUERY)
        assert len(subqueries) == 0

    def test_get_executable_components(self, sample_result):
        """Test getting only executable components."""
        executable = sample_result.get_executable_components()
        assert len(executable) == 2  # CTE and SOURCE_QUERY
        assert all(c.is_executable for c in executable)
