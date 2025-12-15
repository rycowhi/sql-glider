"""Tests for GraphBuilder class."""

from pathlib import Path

import pytest

from sqlglider.global_models import NodeFormat
from sqlglider.graph.builder import GraphBuilder


class TestGraphBuilderSingleFile:
    """Tests for single file processing."""

    def test_add_simple_query(self, tmp_path):
        """Test adding a simple query file."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT customer_id, customer_name FROM customers;")

        builder = GraphBuilder()
        builder.add_file(sql_file)
        graph = builder.build()

        assert graph.metadata.total_nodes > 0
        assert len(graph.metadata.source_files) == 1

    def test_add_query_with_join(self, tmp_path):
        """Test adding a query with joins creates edges."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            SELECT
                c.customer_name,
                o.order_total
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
        """)

        builder = GraphBuilder()
        builder.add_file(sql_file)
        graph = builder.build()

        # Should have nodes for output columns and source columns
        assert graph.metadata.total_nodes > 0
        assert graph.metadata.total_edges > 0

    def test_dialect_option(self, tmp_path):
        """Test specifying dialect."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")

        builder = GraphBuilder(dialect="postgres")
        builder.add_file(sql_file)
        graph = builder.build()

        assert graph.metadata.default_dialect == "postgres"

    def test_node_format_option(self, tmp_path):
        """Test specifying node format."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")

        builder = GraphBuilder(node_format=NodeFormat.STRUCTURED)
        builder.add_file(sql_file)
        graph = builder.build()

        assert graph.metadata.node_format == NodeFormat.STRUCTURED

    def test_method_chaining(self, tmp_path):
        """Test builder supports method chaining."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")

        graph = GraphBuilder().add_file(sql_file).build()
        assert graph is not None


class TestGraphBuilderMultipleFiles:
    """Tests for multiple file processing."""

    def test_add_files_list(self, tmp_path):
        """Test adding multiple files via list."""
        file1 = tmp_path / "query1.sql"
        file1.write_text("SELECT id FROM table1;")

        file2 = tmp_path / "query2.sql"
        file2.write_text("SELECT name FROM table2;")

        builder = GraphBuilder()
        builder.add_files([file1, file2])
        graph = builder.build()

        assert len(graph.metadata.source_files) == 2

    def test_node_deduplication(self, tmp_path):
        """Test that same column from multiple files creates one node."""
        # Both files reference same table.column
        file1 = tmp_path / "query1.sql"
        file1.write_text("SELECT customer_id FROM customers;")

        file2 = tmp_path / "query2.sql"
        file2.write_text("SELECT customer_id FROM customers;")

        builder = GraphBuilder()
        builder.add_files([file1, file2])
        graph = builder.build()

        # Should have deduplicated nodes
        identifiers = [n.identifier for n in graph.nodes]
        # customers.customer_id should appear only once
        assert identifiers.count("customers.customer_id") == 1


class TestGraphBuilderDirectory:
    """Tests for directory processing."""

    def test_add_directory(self, tmp_path):
        """Test adding directory with SQL files."""
        (tmp_path / "query1.sql").write_text("SELECT id FROM table1;")
        (tmp_path / "query2.sql").write_text("SELECT name FROM table2;")
        (tmp_path / "readme.txt").write_text("Not a SQL file")

        builder = GraphBuilder()
        builder.add_directory(tmp_path)
        graph = builder.build()

        # Should have processed only .sql files
        assert len(graph.metadata.source_files) == 2

    def test_add_directory_recursive(self, tmp_path):
        """Test recursive directory search."""
        # Create subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        (tmp_path / "query1.sql").write_text("SELECT id FROM table1;")
        (subdir / "query2.sql").write_text("SELECT name FROM table2;")

        # Non-recursive should only find root file
        builder1 = GraphBuilder()
        builder1.add_directory(tmp_path, recursive=False)
        graph1 = builder1.build()
        assert len(graph1.metadata.source_files) == 1

        # Recursive should find both
        builder2 = GraphBuilder()
        builder2.add_directory(tmp_path, recursive=True)
        graph2 = builder2.build()
        assert len(graph2.metadata.source_files) == 2

    def test_add_directory_custom_glob(self, tmp_path):
        """Test custom glob pattern."""
        (tmp_path / "query.sql").write_text("SELECT id FROM table1;")
        (tmp_path / "query.spark.sql").write_text("SELECT name FROM table2;")

        builder = GraphBuilder()
        builder.add_directory(tmp_path, glob_pattern="*.spark.sql")
        graph = builder.build()

        assert len(graph.metadata.source_files) == 1

    def test_add_directory_not_a_directory(self, tmp_path):
        """Test error when path is not a directory."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM table1;")

        builder = GraphBuilder()
        with pytest.raises(ValueError) as exc_info:
            builder.add_directory(sql_file)
        assert "Not a directory" in str(exc_info.value)


class TestGraphBuilderManifest:
    """Tests for manifest file processing."""

    def test_add_manifest(self, tmp_path):
        """Test adding files from manifest."""
        # Create SQL files
        (tmp_path / "query1.sql").write_text("SELECT id FROM table1;")
        (tmp_path / "query2.sql").write_text("SELECT name FROM table2;")

        # Create manifest
        manifest = tmp_path / "manifest.csv"
        manifest.write_text(
            "file_path,dialect\nquery1.sql,spark\nquery2.sql,postgres\n"
        )

        builder = GraphBuilder()
        builder.add_manifest(manifest)
        graph = builder.build()

        assert len(graph.metadata.source_files) == 2

    def test_add_manifest_with_relative_paths(self, tmp_path):
        """Test manifest with relative file paths."""
        # Create subdirectory with SQL files
        subdir = tmp_path / "queries"
        subdir.mkdir()
        (subdir / "query.sql").write_text("SELECT id FROM table1;")

        # Create manifest in root
        manifest = tmp_path / "manifest.csv"
        manifest.write_text("file_path,dialect\nqueries/query.sql,spark\n")

        builder = GraphBuilder()
        builder.add_manifest(manifest)
        graph = builder.build()

        assert len(graph.metadata.source_files) == 1

    def test_add_manifest_dialect_fallback(self, tmp_path):
        """Test manifest entries without dialect use default."""
        (tmp_path / "query.sql").write_text("SELECT id FROM table1;")

        manifest = tmp_path / "manifest.csv"
        manifest.write_text("file_path,dialect\nquery.sql,\n")

        builder = GraphBuilder(dialect="postgres")
        builder.add_manifest(manifest)
        graph = builder.build()

        assert graph.metadata.default_dialect == "postgres"


class TestGraphBuilderEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_graph(self):
        """Test building graph with no files."""
        builder = GraphBuilder()
        graph = builder.build()

        assert graph.metadata.total_nodes == 0
        assert graph.metadata.total_edges == 0
        assert len(graph.metadata.source_files) == 0

    def test_file_not_found(self):
        """Test error when file doesn't exist."""
        builder = GraphBuilder()
        with pytest.raises(FileNotFoundError):
            builder.add_file(Path("/nonexistent/query.sql"))

    def test_edge_deduplication(self, tmp_path):
        """Test that duplicate edges are not created."""
        sql_file = tmp_path / "query.sql"
        # Same lineage relationship appears twice
        sql_file.write_text("""
            SELECT customer_id FROM customers;
            SELECT customer_id FROM customers;
        """)

        builder = GraphBuilder()
        builder.add_file(sql_file)
        graph = builder.build()

        # Edges should be deduplicated
        edge_pairs = [(e.source_node, e.target_node) for e in graph.edges]
        assert len(edge_pairs) == len(set(edge_pairs))

    def test_rustworkx_graph_property(self, tmp_path):
        """Test access to underlying rustworkx graph."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")

        builder = GraphBuilder()
        builder.add_file(sql_file)

        rx_graph = builder.rustworkx_graph
        assert rx_graph is not None
        assert rx_graph.num_nodes() > 0

    def test_node_index_map_property(self, tmp_path):
        """Test access to node index map."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT customer_id FROM customers;")

        builder = GraphBuilder()
        builder.add_file(sql_file)

        node_map = builder.node_index_map
        assert isinstance(node_map, dict)
        assert len(node_map) > 0

    def test_skip_non_select_statements(self, tmp_path):
        """Test that non-SELECT statements are gracefully skipped with warnings."""
        # Create a DELETE statement file
        delete_file = tmp_path / "delete.sql"
        delete_file.write_text("DELETE FROM customers WHERE customer_id = 123;")

        # Create a valid SELECT file
        select_file = tmp_path / "select.sql"
        select_file.write_text("SELECT * FROM orders;")

        # Create an UPDATE statement file
        update_file = tmp_path / "update.sql"
        update_file.write_text("UPDATE products SET price = 10 WHERE product_id = 1;")

        builder = GraphBuilder()
        builder.add_file(delete_file)
        builder.add_file(select_file)
        builder.add_file(update_file)

        graph = builder.build()

        # Only the SELECT file should have nodes in the graph
        assert graph.metadata.total_nodes > 0
        # All files are parsed successfully and added to source_files
        # (individual queries within are skipped, not entire files)
        assert len(graph.metadata.source_files) == 3
        assert str(select_file.resolve()) in graph.metadata.source_files

    def test_skipped_files_property(self, tmp_path):
        """Test access to skipped files list - now empty since queries are skipped individually."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("TRUNCATE TABLE customers;")

        builder = GraphBuilder()
        builder.add_file(sql_file)

        # With the new behavior, individual queries are skipped (with warnings)
        # but the file itself is not added to skipped_files since parsing succeeded.
        # The skipped_files property only tracks files that fail completely (e.g., parse errors).
        skipped = builder.skipped_files
        assert isinstance(skipped, list)
        # File is processed but queries within are skipped - file not in skipped_files
        assert len(skipped) == 0
