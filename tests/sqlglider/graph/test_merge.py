"""Tests for GraphMerger class."""

from pathlib import Path

import pytest

from sqlglider.graph.merge import GraphMerger, merge_graphs
from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
)
from sqlglider.graph.serialization import save_graph


class TestGraphMergerBasic:
    """Basic tests for GraphMerger."""

    def test_merge_empty_graphs(self):
        """Test merging empty graphs."""
        graph1 = LineageGraph()
        graph2 = LineageGraph()

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merged = merger.merge()

        assert merged.metadata.total_nodes == 0
        assert merged.metadata.total_edges == 0

    def test_merge_two_graphs(self):
        """Test merging two graphs."""
        nodes1 = [
            GraphNode.from_identifier("source.col", "/path/q1.sql", 0),
            GraphNode.from_identifier("table1.col1", "/path/q1.sql", 0),
        ]
        edges1 = [
            GraphEdge(
                source_node="source.col",
                target_node="table1.col1",
                file_path="/path/q1.sql",
                query_index=0,
            )
        ]
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q1.sql"]),
            nodes=nodes1,
            edges=edges1,
        )

        nodes2 = [
            GraphNode.from_identifier("source.col2", "/path/q2.sql", 0),
            GraphNode.from_identifier("table2.col2", "/path/q2.sql", 0),
        ]
        edges2 = [
            GraphEdge(
                source_node="source.col2",
                target_node="table2.col2",
                file_path="/path/q2.sql",
                query_index=0,
            )
        ]
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q2.sql"]),
            nodes=nodes2,
            edges=edges2,
        )

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merged = merger.merge()

        # Should have nodes from both graphs (4 total)
        assert merged.metadata.total_nodes == 4
        assert merged.metadata.total_edges == 2
        assert len(merged.metadata.source_files) == 2


class TestGraphMergerDeduplication:
    """Tests for node and edge deduplication."""

    def test_node_deduplication(self):
        """Test that nodes with same identifier are deduplicated."""
        # Both graphs have same node identifier
        nodes1 = [
            GraphNode.from_identifier("shared.column", "/path/q1.sql", 0),
        ]
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q1.sql"]),
            nodes=nodes1,
        )

        nodes2 = [
            GraphNode.from_identifier("shared.column", "/path/q2.sql", 1),
        ]
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q2.sql"]),
            nodes=nodes2,
        )

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merged = merger.merge()

        # Should only have one node
        assert merged.metadata.total_nodes == 1
        # First occurrence wins
        node = merged.nodes[0]
        assert node.file_path == "/path/q1.sql"
        assert node.query_index == 0

    def test_edge_deduplication(self):
        """Test that edges with same source-target are deduplicated."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/q1.sql", 0),
            GraphNode.from_identifier("target.col", "/path/q1.sql", 0),
        ]
        edge = GraphEdge(
            source_node="source.col",
            target_node="target.col",
            file_path="/path/q1.sql",
            query_index=0,
        )

        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q1.sql"]),
            nodes=nodes,
            edges=[edge],
        )

        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q2.sql"]),
            nodes=nodes,
            edges=[edge],  # Same edge
        )

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merged = merger.merge()

        # Should only have one edge
        assert merged.metadata.total_edges == 1


class TestGraphMergerFileOperations:
    """Tests for file-based merge operations."""

    def test_add_file(self, tmp_path):
        """Test adding graph from JSON file."""
        graph = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/query.sql"]),
            nodes=[GraphNode.from_identifier("table.col", "/path/query.sql", 0)],
        )

        graph_file = tmp_path / "graph.json"
        save_graph(graph, graph_file)

        merger = GraphMerger()
        merger.add_file(graph_file)
        merged = merger.merge()

        assert merged.metadata.total_nodes == 1

    def test_add_files(self, tmp_path):
        """Test adding multiple graph files."""
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q1.sql"]),
            nodes=[GraphNode.from_identifier("t1.col", "/path/q1.sql", 0)],
        )
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q2.sql"]),
            nodes=[GraphNode.from_identifier("t2.col", "/path/q2.sql", 0)],
        )

        path1 = tmp_path / "graph1.json"
        path2 = tmp_path / "graph2.json"

        save_graph(graph1, path1)
        save_graph(graph2, path2)

        merger = GraphMerger()
        merger.add_files([path1, path2])
        merged = merger.merge()

        assert merged.metadata.total_nodes == 2
        assert len(merged.metadata.source_files) == 2

    def test_add_nonexistent_file(self):
        """Test error when adding non-existent file."""
        merger = GraphMerger()
        with pytest.raises(FileNotFoundError):
            merger.add_file(Path("/nonexistent/graph.json"))


class TestGraphMergerMethodChaining:
    """Tests for method chaining support."""

    def test_method_chaining(self, tmp_path):
        """Test that all add methods return self."""
        graph = LineageGraph()

        graph_file = tmp_path / "graph.json"
        save_graph(graph, graph_file)

        result = GraphMerger().add_graph(graph).add_file(graph_file).merge()

        assert result is not None


class TestMergeGraphsFunction:
    """Tests for the convenience merge_graphs function."""

    def test_merge_graphs_convenience_function(self, tmp_path):
        """Test merge_graphs convenience function."""
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q1.sql"]),
            nodes=[GraphNode.from_identifier("t1.col", "/path/q1.sql", 0)],
        )
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/q2.sql"]),
            nodes=[GraphNode.from_identifier("t2.col", "/path/q2.sql", 0)],
        )

        path1 = tmp_path / "graph1.json"
        path2 = tmp_path / "graph2.json"

        save_graph(graph1, path1)
        save_graph(graph2, path2)

        merged = merge_graphs([path1, path2])

        assert merged.metadata.total_nodes == 2


class TestGraphMergerSourceFiles:
    """Tests for source file tracking in merged graphs."""

    def test_source_files_aggregated(self):
        """Test that source files are aggregated from all graphs."""
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/a.sql", "/path/b.sql"]),
        )
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/c.sql"]),
        )
        graph3 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/a.sql"]),  # Duplicate
        )

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merger.add_graph(graph3)
        merged = merger.merge()

        # Should have unique source files
        assert len(merged.metadata.source_files) == 3
        assert "/path/a.sql" in merged.metadata.source_files
        assert "/path/b.sql" in merged.metadata.source_files
        assert "/path/c.sql" in merged.metadata.source_files

    def test_source_files_sorted(self):
        """Test that source files are sorted in output."""
        graph1 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/z.sql"]),
        )
        graph2 = LineageGraph(
            metadata=GraphMetadata(source_files=["/path/a.sql"]),
        )

        merger = GraphMerger()
        merger.add_graph(graph1)
        merger.add_graph(graph2)
        merged = merger.merge()

        assert merged.metadata.source_files == ["/path/a.sql", "/path/z.sql"]
