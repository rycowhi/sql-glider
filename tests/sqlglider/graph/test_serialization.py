"""Tests for graph serialization."""

from pathlib import Path

import pytest
import rustworkx as rx

from sqlglider.global_models import NodeFormat
from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
)
from sqlglider.graph.serialization import (
    from_rustworkx,
    load_graph,
    save_graph,
    to_rustworkx,
)


class TestSaveAndLoadGraph:
    """Tests for save_graph and load_graph functions."""

    def test_save_and_load_roundtrip(self, tmp_path):
        """Test saving and loading graph preserves data."""
        nodes = [
            GraphNode.from_identifier("orders.id", "/path/query.sql", 0),
            GraphNode.from_identifier("orders.customer_id", "/path/query.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="customers.id",
                target_node="orders.customer_id",
                file_path="/path/query.sql",
                query_index=0,
            )
        ]
        original = LineageGraph(
            metadata=GraphMetadata(
                node_format=NodeFormat.QUALIFIED,
                default_dialect="spark",
                source_files=["/path/query.sql"],
                total_nodes=2,
                total_edges=1,
            ),
            nodes=nodes,
            edges=edges,
        )

        graph_file = tmp_path / "graph.json"
        save_graph(original, graph_file)
        loaded = load_graph(graph_file)

        assert loaded.metadata.node_format == original.metadata.node_format
        assert loaded.metadata.default_dialect == original.metadata.default_dialect
        assert len(loaded.nodes) == len(original.nodes)
        assert len(loaded.edges) == len(original.edges)
        assert loaded.nodes[0].identifier == original.nodes[0].identifier
        assert loaded.edges[0].source_node == original.edges[0].source_node

    def test_load_nonexistent_file(self):
        """Test loading from non-existent file raises error."""
        with pytest.raises(FileNotFoundError):
            load_graph(Path("/nonexistent/graph.json"))

    def test_save_creates_file(self, tmp_path):
        """Test save_graph creates the output file."""
        graph = LineageGraph()

        graph_file = tmp_path / "new_graph.json"
        assert not graph_file.exists()
        save_graph(graph, graph_file)
        assert graph_file.exists()


class TestRustworkxConversion:
    """Tests for to_rustworkx and from_rustworkx functions."""

    def test_to_rustworkx_empty_graph(self):
        """Test converting empty graph to rustworkx."""
        graph = LineageGraph()
        rx_graph, node_map = to_rustworkx(graph)

        assert isinstance(rx_graph, rx.PyDiGraph)
        assert len(node_map) == 0
        assert rx_graph.num_nodes() == 0
        assert rx_graph.num_edges() == 0

    def test_to_rustworkx_with_nodes_and_edges(self):
        """Test converting graph with data to rustworkx."""
        nodes = [
            GraphNode.from_identifier("source.col", "/path/query.sql", 0),
            GraphNode.from_identifier("target.col", "/path/query.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="source.col",
                target_node="target.col",
                file_path="/path/query.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)

        rx_graph, node_map = to_rustworkx(graph)

        assert rx_graph.num_nodes() == 2
        assert rx_graph.num_edges() == 1
        assert "source.col" in node_map
        assert "target.col" in node_map

        # Verify edge exists
        source_idx = node_map["source.col"]
        target_idx = node_map["target.col"]
        assert rx_graph.has_edge(source_idx, target_idx)

    def test_from_rustworkx_empty_graph(self):
        """Test converting empty rustworkx graph."""
        rx_graph = rx.PyDiGraph()
        metadata = GraphMetadata()

        graph = from_rustworkx(rx_graph, metadata)

        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0
        assert graph.metadata.total_nodes == 0
        assert graph.metadata.total_edges == 0

    def test_from_rustworkx_with_data(self):
        """Test converting rustworkx graph with data."""
        rx_graph = rx.PyDiGraph()

        node1_data = GraphNode.from_identifier(
            "source.col", "/path/query.sql", 0
        ).model_dump()
        node2_data = GraphNode.from_identifier(
            "target.col", "/path/query.sql", 0
        ).model_dump()
        edge_data = GraphEdge(
            source_node="source.col",
            target_node="target.col",
            file_path="/path/query.sql",
            query_index=0,
        ).model_dump()

        idx1 = rx_graph.add_node(node1_data)
        idx2 = rx_graph.add_node(node2_data)
        rx_graph.add_edge(idx1, idx2, edge_data)

        metadata = GraphMetadata(source_files=["/path/query.sql"])
        graph = from_rustworkx(rx_graph, metadata)

        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1
        assert graph.metadata.total_nodes == 2
        assert graph.metadata.total_edges == 1

    def test_roundtrip_conversion(self):
        """Test converting LineageGraph -> rustworkx -> LineageGraph."""
        nodes = [
            GraphNode.from_identifier("a.col1", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col2", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col3", "/path/q.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="a.col1",
                target_node="b.col2",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col2",
                target_node="c.col3",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ]
        original = LineageGraph(nodes=nodes, edges=edges)

        # Convert to rustworkx
        rx_graph, node_map = to_rustworkx(original)

        # Convert back
        restored = from_rustworkx(rx_graph, original.metadata)

        assert len(restored.nodes) == len(original.nodes)
        assert len(restored.edges) == len(original.edges)

        # Verify node identifiers match
        original_ids = {n.identifier for n in original.nodes}
        restored_ids = {n.identifier for n in restored.nodes}
        assert original_ids == restored_ids
