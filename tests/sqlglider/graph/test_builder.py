"""Tests for GraphBuilder class."""

from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from sqlglider.graph.builder import GraphBuilder


class TestGraphBuilderSingleFile:
    """Tests for single file processing."""

    def test_add_simple_query(self):
        """Test adding a simple query file."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT customer_id, customer_name FROM customers;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)
            graph = builder.build()

            assert graph.metadata.total_nodes > 0
            assert len(graph.metadata.source_files) == 1
        finally:
            temp_path.unlink()

    def test_add_query_with_join(self):
        """Test adding a query with joins creates edges."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("""
                SELECT
                    c.customer_name,
                    o.order_total
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
            """)
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)
            graph = builder.build()

            # Should have nodes for output columns and source columns
            assert graph.metadata.total_nodes > 0
            assert graph.metadata.total_edges > 0
        finally:
            temp_path.unlink()

    def test_dialect_option(self):
        """Test specifying dialect."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT id FROM users;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder(dialect="postgres")
            builder.add_file(temp_path)
            graph = builder.build()

            assert graph.metadata.default_dialect == "postgres"
        finally:
            temp_path.unlink()

    def test_node_format_option(self):
        """Test specifying node format."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT id FROM users;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder(node_format="structured")
            builder.add_file(temp_path)
            graph = builder.build()

            assert graph.metadata.node_format == "structured"
        finally:
            temp_path.unlink()

    def test_method_chaining(self):
        """Test builder supports method chaining."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT id FROM users;")
            temp_path = Path(f.name)

        try:
            graph = GraphBuilder().add_file(temp_path).build()
            assert graph is not None
        finally:
            temp_path.unlink()


class TestGraphBuilderMultipleFiles:
    """Tests for multiple file processing."""

    def test_add_files_list(self):
        """Test adding multiple files via list."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            file1 = tmpdir_path / "query1.sql"
            file1.write_text("SELECT id FROM table1;")

            file2 = tmpdir_path / "query2.sql"
            file2.write_text("SELECT name FROM table2;")

            builder = GraphBuilder()
            builder.add_files([file1, file2])
            graph = builder.build()

            assert len(graph.metadata.source_files) == 2

    def test_node_deduplication(self):
        """Test that same column from multiple files creates one node."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Both files reference same table.column
            file1 = tmpdir_path / "query1.sql"
            file1.write_text("SELECT customer_id FROM customers;")

            file2 = tmpdir_path / "query2.sql"
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

    def test_add_directory(self):
        """Test adding directory with SQL files."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            (tmpdir_path / "query1.sql").write_text("SELECT id FROM table1;")
            (tmpdir_path / "query2.sql").write_text("SELECT name FROM table2;")
            (tmpdir_path / "readme.txt").write_text("Not a SQL file")

            builder = GraphBuilder()
            builder.add_directory(tmpdir_path)
            graph = builder.build()

            # Should have processed only .sql files
            assert len(graph.metadata.source_files) == 2

    def test_add_directory_recursive(self):
        """Test recursive directory search."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create subdirectory
            subdir = tmpdir_path / "subdir"
            subdir.mkdir()

            (tmpdir_path / "query1.sql").write_text("SELECT id FROM table1;")
            (subdir / "query2.sql").write_text("SELECT name FROM table2;")

            # Non-recursive should only find root file
            builder1 = GraphBuilder()
            builder1.add_directory(tmpdir_path, recursive=False)
            graph1 = builder1.build()
            assert len(graph1.metadata.source_files) == 1

            # Recursive should find both
            builder2 = GraphBuilder()
            builder2.add_directory(tmpdir_path, recursive=True)
            graph2 = builder2.build()
            assert len(graph2.metadata.source_files) == 2

    def test_add_directory_custom_glob(self):
        """Test custom glob pattern."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            (tmpdir_path / "query.sql").write_text("SELECT id FROM table1;")
            (tmpdir_path / "query.spark.sql").write_text("SELECT name FROM table2;")

            builder = GraphBuilder()
            builder.add_directory(tmpdir_path, glob_pattern="*.spark.sql")
            graph = builder.build()

            assert len(graph.metadata.source_files) == 1

    def test_add_directory_not_a_directory(self):
        """Test error when path is not a directory."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT id FROM table1;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            with pytest.raises(ValueError) as exc_info:
                builder.add_directory(temp_path)
            assert "Not a directory" in str(exc_info.value)
        finally:
            temp_path.unlink()


class TestGraphBuilderManifest:
    """Tests for manifest file processing."""

    def test_add_manifest(self):
        """Test adding files from manifest."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create SQL files
            (tmpdir_path / "query1.sql").write_text("SELECT id FROM table1;")
            (tmpdir_path / "query2.sql").write_text("SELECT name FROM table2;")

            # Create manifest
            manifest = tmpdir_path / "manifest.csv"
            manifest.write_text(
                "file_path,dialect\nquery1.sql,spark\nquery2.sql,postgres\n"
            )

            builder = GraphBuilder()
            builder.add_manifest(manifest)
            graph = builder.build()

            assert len(graph.metadata.source_files) == 2

    def test_add_manifest_with_relative_paths(self):
        """Test manifest with relative file paths."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Create subdirectory with SQL files
            subdir = tmpdir_path / "queries"
            subdir.mkdir()
            (subdir / "query.sql").write_text("SELECT id FROM table1;")

            # Create manifest in root
            manifest = tmpdir_path / "manifest.csv"
            manifest.write_text("file_path,dialect\nqueries/query.sql,spark\n")

            builder = GraphBuilder()
            builder.add_manifest(manifest)
            graph = builder.build()

            assert len(graph.metadata.source_files) == 1

    def test_add_manifest_dialect_fallback(self):
        """Test manifest entries without dialect use default."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            (tmpdir_path / "query.sql").write_text("SELECT id FROM table1;")

            manifest = tmpdir_path / "manifest.csv"
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

    def test_edge_deduplication(self):
        """Test that duplicate edges are not created."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            # Same lineage relationship appears twice
            f.write("""
                SELECT customer_id FROM customers;
                SELECT customer_id FROM customers;
            """)
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)
            graph = builder.build()

            # Edges should be deduplicated
            edge_pairs = [(e.source_node, e.target_node) for e in graph.edges]
            assert len(edge_pairs) == len(set(edge_pairs))
        finally:
            temp_path.unlink()

    def test_rustworkx_graph_property(self):
        """Test access to underlying rustworkx graph."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT id FROM users;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)

            rx_graph = builder.rustworkx_graph
            assert rx_graph is not None
            assert rx_graph.num_nodes() > 0
        finally:
            temp_path.unlink()

    def test_node_index_map_property(self):
        """Test access to node index map."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("SELECT customer_id FROM customers;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)

            node_map = builder.node_index_map
            assert isinstance(node_map, dict)
            assert len(node_map) > 0
        finally:
            temp_path.unlink()

    def test_skip_non_select_statements(self):
        """Test that non-SELECT statements are gracefully skipped with warnings."""
        with TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)

            # Create a DELETE statement file
            delete_file = temp_dir / "delete.sql"
            delete_file.write_text("DELETE FROM customers WHERE customer_id = 123;")

            # Create a valid SELECT file
            select_file = temp_dir / "select.sql"
            select_file.write_text("SELECT * FROM orders;")

            # Create an UPDATE statement file
            update_file = temp_dir / "update.sql"
            update_file.write_text(
                "UPDATE products SET price = 10 WHERE product_id = 1;"
            )

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

    def test_skipped_files_property(self):
        """Test access to skipped files list - now empty since queries are skipped individually."""
        with NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write("TRUNCATE TABLE customers;")
            temp_path = Path(f.name)

        try:
            builder = GraphBuilder()
            builder.add_file(temp_path)

            # With the new behavior, individual queries are skipped (with warnings)
            # but the file itself is not added to skipped_files since parsing succeeded.
            # The skipped_files property only tracks files that fail completely (e.g., parse errors).
            skipped = builder.skipped_files
            assert isinstance(skipped, list)
            # File is processed but queries within are skipped - file not in skipped_files
            assert len(skipped) == 0
        finally:
            temp_path.unlink()
