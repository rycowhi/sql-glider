"""Tests for GraphQuerier class."""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from sqlglider.graph.models import (
    GraphEdge,
    GraphNode,
    LineageGraph,
    LineageNode,
)
from sqlglider.graph.query import GraphQuerier, LineageQueryResult
from sqlglider.graph.serialization import save_graph


class TestLineageQueryResult:
    """Tests for LineageQueryResult class."""

    def test_basic_result(self):
        """Test basic result creation."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q.sql", 0),
        ]
        result = LineageQueryResult(
            query_column="target.col",
            direction="upstream",
            related_columns=nodes,
        )

        assert result.query_column == "target.col"
        assert result.direction == "upstream"
        assert len(result) == 1

    def test_iteration(self):
        """Test iterating over result."""
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
        ]
        result = LineageQueryResult(
            query_column="target",
            direction="upstream",
            related_columns=nodes,
        )

        collected = list(result)
        assert len(collected) == 2

    def test_empty_result(self):
        """Test empty result."""
        result = LineageQueryResult(
            query_column="isolated.col",
            direction="downstream",
            related_columns=[],
        )

        assert len(result) == 0
        assert list(result) == []


class TestGraphQuerierBasic:
    """Basic tests for GraphQuerier."""

    def test_from_file(self):
        """Test creating querier from file."""
        graph = LineageGraph(
            nodes=[GraphNode.from_identifier("table.col", "/path/q.sql", 0)],
        )

        with NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            temp_path = Path(f.name)

        try:
            save_graph(graph, temp_path)
            querier = GraphQuerier.from_file(temp_path)
            assert querier is not None
        finally:
            temp_path.unlink()

    def test_from_file_not_found(self):
        """Test error when file not found."""
        with pytest.raises(FileNotFoundError):
            GraphQuerier.from_file(Path("/nonexistent/graph.json"))

    def test_list_columns(self):
        """Test listing all columns."""
        nodes = [
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ]
        graph = LineageGraph(nodes=nodes)
        querier = GraphQuerier(graph)

        columns = querier.list_columns()
        assert len(columns) == 3
        # Should be sorted
        assert columns == ["a.col", "b.col", "c.col"]


class TestGraphQuerierUpstream:
    """Tests for upstream (ancestors) queries."""

    def test_simple_upstream(self):
        """Test finding upstream of a single edge."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="source.col",
                target_node="target.col",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("target.col")

        assert result.direction == "upstream"
        assert result.query_column == "target.col"
        assert len(result) == 1
        assert result.related_columns[0].identifier == "source.col"

    def test_transitive_upstream(self):
        """Test finding transitive upstream (multiple hops)."""
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("c.col")

        # Should find both a and b
        identifiers = [n.identifier for n in result.related_columns]
        assert len(identifiers) == 2
        assert "a.col" in identifiers
        assert "b.col" in identifiers

    def test_no_upstream(self):
        """Test column with no upstream sources."""
        nodes = [
            GraphNode.from_identifier("root.col", "/path/q.sql", 0),
        ]
        graph = LineageGraph(nodes=nodes)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("root.col")

        assert len(result) == 0

    def test_upstream_column_not_found(self):
        """Test error when column not in graph."""
        graph = LineageGraph(
            nodes=[GraphNode.from_identifier("table.col", "/path/q.sql", 0)]
        )
        querier = GraphQuerier(graph)

        with pytest.raises(ValueError) as exc_info:
            querier.find_upstream("nonexistent.col")
        assert "not found" in str(exc_info.value)

    def test_upstream_case_insensitive(self):
        """Test case-insensitive column matching for upstream."""
        nodes = [
            GraphNode.from_identifier("Source.Column", "/path/q.sql", 0),
            GraphNode.from_identifier("Target.Column", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="Source.Column",
                target_node="Target.Column",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        # Query with different case
        result = querier.find_upstream("TARGET.COLUMN")

        assert result.query_column == "Target.Column"  # Original case preserved
        assert len(result) == 1


class TestGraphQuerierDownstream:
    """Tests for downstream (descendants) queries."""

    def test_simple_downstream(self):
        """Test finding downstream of a single edge."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="source.col",
                target_node="target.col",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_downstream("source.col")

        assert result.direction == "downstream"
        assert result.query_column == "source.col"
        assert len(result) == 1
        assert result.related_columns[0].identifier == "target.col"

    def test_transitive_downstream(self):
        """Test finding transitive downstream (multiple hops)."""
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_downstream("a.col")

        # Should find both b and c
        identifiers = [n.identifier for n in result.related_columns]
        assert len(identifiers) == 2
        assert "b.col" in identifiers
        assert "c.col" in identifiers

    def test_no_downstream(self):
        """Test column with no downstream dependents."""
        nodes = [
            GraphNode.from_identifier("leaf.col", "/path/q.sql", 0),
        ]
        graph = LineageGraph(nodes=nodes)
        querier = GraphQuerier(graph)

        result = querier.find_downstream("leaf.col")

        assert len(result) == 0

    def test_downstream_column_not_found(self):
        """Test error when column not in graph."""
        graph = LineageGraph(
            nodes=[GraphNode.from_identifier("table.col", "/path/q.sql", 0)]
        )
        querier = GraphQuerier(graph)

        with pytest.raises(ValueError) as exc_info:
            querier.find_downstream("nonexistent.col")
        assert "not found" in str(exc_info.value)

    def test_downstream_case_insensitive(self):
        """Test case-insensitive column matching for downstream."""
        nodes = [
            GraphNode.from_identifier("Source.Column", "/path/q.sql", 0),
            GraphNode.from_identifier("Target.Column", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="Source.Column",
                target_node="Target.Column",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        # Query with different case
        result = querier.find_downstream("source.column")

        assert result.query_column == "Source.Column"  # Original case preserved
        assert len(result) == 1


class TestGraphQuerierComplexGraph:
    """Tests with more complex graph structures."""

    def test_diamond_dependency(self):
        """Test diamond-shaped dependency graph."""
        # A -> B, A -> C, B -> D, C -> D
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
            GraphNode.from_identifier("d.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="a.col",
                target_node="c.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="d.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="c.col",
                target_node="d.col",
                file_path="/p.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        # Upstream of D should include A, B, C
        upstream = querier.find_upstream("d.col")
        upstream_ids = [n.identifier for n in upstream.related_columns]
        assert len(upstream_ids) == 3
        assert "a.col" in upstream_ids
        assert "b.col" in upstream_ids
        assert "c.col" in upstream_ids

        # Downstream of A should include B, C, D
        downstream = querier.find_downstream("a.col")
        downstream_ids = [n.identifier for n in downstream.related_columns]
        assert len(downstream_ids) == 3
        assert "b.col" in downstream_ids
        assert "c.col" in downstream_ids
        assert "d.col" in downstream_ids

    def test_multiple_sources(self):
        """Test column with multiple direct sources."""
        nodes = [
            GraphNode.from_identifier("src1.col", "/path/q.sql", 0),
            GraphNode.from_identifier("src2.col", "/path/q.sql", 0),
            GraphNode.from_identifier("src3.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="src1.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="src2.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="src3.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("target.col")
        assert len(result) == 3

    def test_results_sorted(self):
        """Test that results are sorted by identifier."""
        nodes = [
            GraphNode.from_identifier("z.col", "/path/q.sql", 0),
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("m.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="z.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="a.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="m.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("target.col")
        identifiers = [n.identifier for n in result.related_columns]

        # Should be sorted alphabetically (case-insensitive)
        assert identifiers == ["a.col", "m.col", "z.col"]


class TestLineageNodeModel:
    """Tests for LineageNode Pydantic model."""

    def test_from_graph_node(self):
        """Test creating LineageNode from GraphNode."""
        graph_node = GraphNode.from_identifier("table.col", "/path/q.sql", 0)
        lineage_node = LineageNode.from_graph_node(
            graph_node, hops=2, output_column="target.col"
        )

        assert lineage_node.identifier == "table.col"
        assert lineage_node.table == "table"
        assert lineage_node.column == "col"
        assert lineage_node.file_path == "/path/q.sql"
        assert lineage_node.query_index == 0
        assert lineage_node.hops == 2
        assert lineage_node.output_column == "target.col"

    def test_model_dump_includes_hops_and_output(self):
        """Test that model_dump includes hops and output_column."""
        graph_node = GraphNode.from_identifier("schema.table.col", "/path/q.sql", 1)
        lineage_node = LineageNode.from_graph_node(
            graph_node, hops=3, output_column="out.col"
        )

        data = lineage_node.model_dump()

        assert data["hops"] == 3
        assert data["output_column"] == "out.col"
        assert data["identifier"] == "schema.table.col"
        assert data["schema_name"] == "schema"


class TestHopCounting:
    """Tests for hop distance tracking in query results."""

    def test_single_hop_upstream(self):
        """Test hop count for direct upstream dependency."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="source.col",
                target_node="target.col",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("target.col")

        assert len(result) == 1
        assert result.related_columns[0].hops == 1
        assert result.related_columns[0].output_column == "target.col"

    def test_single_hop_downstream(self):
        """Test hop count for direct downstream dependency."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="source.col",
                target_node="target.col",
                file_path="/path/q.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_downstream("source.col")

        assert len(result) == 1
        assert result.related_columns[0].hops == 1
        assert result.related_columns[0].output_column == "source.col"

    def test_multiple_hops_upstream(self):
        """Test hop counts for transitive upstream dependencies."""
        # a -> b -> c (chain)
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("c.col")

        # Sort by hops for predictable ordering in assertions
        by_hops = sorted(result.related_columns, key=lambda n: n.hops)

        assert len(by_hops) == 2
        assert by_hops[0].identifier == "b.col"
        assert by_hops[0].hops == 1
        assert by_hops[1].identifier == "a.col"
        assert by_hops[1].hops == 2

        # All should have same output_column
        for node in result.related_columns:
            assert node.output_column == "c.col"

    def test_multiple_hops_downstream(self):
        """Test hop counts for transitive downstream dependencies."""
        # a -> b -> c (chain)
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_downstream("a.col")

        # Sort by hops for predictable ordering in assertions
        by_hops = sorted(result.related_columns, key=lambda n: n.hops)

        assert len(by_hops) == 2
        assert by_hops[0].identifier == "b.col"
        assert by_hops[0].hops == 1
        assert by_hops[1].identifier == "c.col"
        assert by_hops[1].hops == 2

        # All should have same output_column
        for node in result.related_columns:
            assert node.output_column == "a.col"

    def test_diamond_hop_counts(self):
        """Test hop counts in diamond-shaped dependency graph."""
        # A -> B, A -> C, B -> D, C -> D
        # From D: A is 2 hops via B or via C
        nodes = [
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
            GraphNode.from_identifier("d.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="a.col",
                target_node="c.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="d.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="c.col",
                target_node="d.col",
                file_path="/p.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("d.col")
        hops_map = {n.identifier: n.hops for n in result.related_columns}

        # B and C are 1 hop from D
        assert hops_map["b.col"] == 1
        assert hops_map["c.col"] == 1
        # A is 2 hops from D (shortest path via either B or C)
        assert hops_map["a.col"] == 2

    def test_output_column_preserved_for_all_nodes(self):
        """Test that output_column is consistent across all result nodes."""
        nodes = [
            GraphNode.from_identifier("src1.col", "/path/q.sql", 0),
            GraphNode.from_identifier("src2.col", "/path/q.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="src1.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="src2.col",
                target_node="target.col",
                file_path="/p.sql",
                query_index=0,
            ),
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)
        querier = GraphQuerier(graph)

        result = querier.find_upstream("target.col")

        assert len(result) == 2
        for node in result.related_columns:
            assert node.output_column == "target.col"
