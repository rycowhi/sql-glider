"""Tests for graph Pydantic models."""

from pathlib import Path

import pytest

from sqlglider.global_models import NodeFormat
from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
    Manifest,
    ManifestEntry,
)


class TestGraphNode:
    """Tests for GraphNode model."""

    def test_basic_creation(self):
        """Test basic node creation."""
        node = GraphNode(
            identifier="orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
            table="orders",
            column="customer_id",
        )
        assert node.identifier == "orders.customer_id"
        assert node.file_path == "/path/to/query.sql"
        assert node.query_index == 0
        assert node.table == "orders"
        assert node.column == "customer_id"
        assert node.schema_name is None

    def test_from_identifier_two_parts(self):
        """Test creating node from two-part identifier."""
        node = GraphNode.from_identifier(
            identifier="orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
        )
        assert node.identifier == "orders.customer_id"
        assert node.schema_name is None
        assert node.table == "orders"
        assert node.column == "customer_id"

    def test_from_identifier_three_parts(self):
        """Test creating node from three-part identifier."""
        node = GraphNode.from_identifier(
            identifier="schema.orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=1,
        )
        assert node.identifier == "schema.orders.customer_id"
        assert node.schema_name == "schema"
        assert node.table == "orders"
        assert node.column == "customer_id"

    def test_from_identifier_single_part(self):
        """Test creating node from single-part identifier."""
        node = GraphNode.from_identifier(
            identifier="customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
        )
        assert node.identifier == "customer_id"
        assert node.schema_name is None
        assert node.table is None
        assert node.column == "customer_id"

    def test_serialization_roundtrip(self):
        """Test JSON serialization roundtrip."""
        node = GraphNode.from_identifier(
            identifier="orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
        )
        json_str = node.model_dump_json()
        restored = GraphNode.model_validate_json(json_str)
        assert restored == node


class TestGraphEdge:
    """Tests for GraphEdge model."""

    def test_basic_creation(self):
        """Test basic edge creation."""
        edge = GraphEdge(
            source_node="customers.customer_id",
            target_node="orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
        )
        assert edge.source_node == "customers.customer_id"
        assert edge.target_node == "orders.customer_id"
        assert edge.file_path == "/path/to/query.sql"
        assert edge.query_index == 0

    def test_serialization_roundtrip(self):
        """Test JSON serialization roundtrip."""
        edge = GraphEdge(
            source_node="customers.customer_id",
            target_node="orders.customer_id",
            file_path="/path/to/query.sql",
            query_index=0,
        )
        json_str = edge.model_dump_json()
        restored = GraphEdge.model_validate_json(json_str)
        assert restored == edge


class TestManifest:
    """Tests for Manifest model."""

    def test_from_csv_basic(self, tmp_path):
        """Test loading manifest from CSV."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_path,dialect\nquery1.sql,spark\nquery2.sql,postgres\nquery3.sql,\n"
        )

        manifest = Manifest.from_csv(csv_file)
        assert len(manifest.entries) == 3
        assert manifest.entries[0].file_path == "query1.sql"
        assert manifest.entries[0].dialect == "spark"
        assert manifest.entries[1].file_path == "query2.sql"
        assert manifest.entries[1].dialect == "postgres"
        assert manifest.entries[2].file_path == "query3.sql"
        assert manifest.entries[2].dialect is None

    def test_from_csv_missing_file(self):
        """Test loading manifest from non-existent file."""
        with pytest.raises(FileNotFoundError):
            Manifest.from_csv(Path("/nonexistent/manifest.csv"))

    def test_from_csv_missing_column(self, tmp_path):
        """Test loading manifest without file_path column."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text("name,dialect\nquery1.sql,spark\n")

        with pytest.raises(ValueError) as exc_info:
            Manifest.from_csv(csv_file)
        assert "file_path" in str(exc_info.value)

    def test_from_csv_skips_empty_rows(self, tmp_path):
        """Test that empty file_path rows are skipped."""
        csv_file = tmp_path / "manifest.csv"
        csv_file.write_text(
            "file_path,dialect\nquery1.sql,spark\n,postgres\nquery2.sql,\n"
        )

        manifest = Manifest.from_csv(csv_file)
        assert len(manifest.entries) == 2
        assert manifest.entries[0].file_path == "query1.sql"
        assert manifest.entries[1].file_path == "query2.sql"


class TestManifestEntry:
    """Tests for ManifestEntry model."""

    def test_basic_creation(self):
        """Test basic entry creation."""
        entry = ManifestEntry(file_path="query.sql", dialect="spark")
        assert entry.file_path == "query.sql"
        assert entry.dialect == "spark"

    def test_optional_dialect(self):
        """Test entry without dialect."""
        entry = ManifestEntry(file_path="query.sql")
        assert entry.file_path == "query.sql"
        assert entry.dialect is None


class TestGraphMetadata:
    """Tests for GraphMetadata model."""

    def test_default_values(self):
        """Test default metadata values."""
        metadata = GraphMetadata()
        assert metadata.node_format == NodeFormat.QUALIFIED
        assert metadata.default_dialect == "spark"
        assert metadata.source_files == []
        assert metadata.total_nodes == 0
        assert metadata.total_edges == 0
        assert metadata.created_at is not None

    def test_custom_values(self):
        """Test custom metadata values."""
        metadata = GraphMetadata(
            node_format=NodeFormat.STRUCTURED,
            default_dialect="postgres",
            source_files=["query1.sql", "query2.sql"],
            total_nodes=10,
            total_edges=15,
        )
        assert metadata.node_format == NodeFormat.STRUCTURED
        assert metadata.default_dialect == "postgres"
        assert len(metadata.source_files) == 2
        assert metadata.total_nodes == 10
        assert metadata.total_edges == 15


class TestLineageGraph:
    """Tests for LineageGraph model."""

    def test_empty_graph(self):
        """Test creating empty graph."""
        graph = LineageGraph()
        assert graph.metadata is not None
        assert graph.nodes == []
        assert graph.edges == []

    def test_graph_with_data(self):
        """Test creating graph with nodes and edges."""
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
        graph = LineageGraph(nodes=nodes, edges=edges)
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1

    def test_get_node_by_identifier(self):
        """Test finding node by identifier."""
        nodes = [
            GraphNode.from_identifier("orders.id", "/path/query.sql", 0),
            GraphNode.from_identifier("orders.customer_id", "/path/query.sql", 0),
        ]
        graph = LineageGraph(nodes=nodes)

        # Exact match
        node = graph.get_node_by_identifier("orders.id")
        assert node is not None
        assert node.identifier == "orders.id"

        # Case-insensitive match
        node = graph.get_node_by_identifier("ORDERS.ID")
        assert node is not None
        assert node.identifier == "orders.id"

        # Not found
        node = graph.get_node_by_identifier("nonexistent.column")
        assert node is None

    def test_serialization_roundtrip(self):
        """Test JSON serialization roundtrip."""
        nodes = [
            GraphNode.from_identifier("orders.id", "/path/query.sql", 0),
        ]
        edges = [
            GraphEdge(
                source_node="customers.id",
                target_node="orders.id",
                file_path="/path/query.sql",
                query_index=0,
            )
        ]
        graph = LineageGraph(nodes=nodes, edges=edges)

        json_str = graph.model_dump_json()
        restored = LineageGraph.model_validate_json(json_str)

        assert len(restored.nodes) == len(graph.nodes)
        assert len(restored.edges) == len(graph.edges)
        assert restored.nodes[0].identifier == graph.nodes[0].identifier
