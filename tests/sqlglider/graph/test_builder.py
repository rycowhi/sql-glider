"""Tests for GraphBuilder class."""

from pathlib import Path
from unittest.mock import MagicMock, patch

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


class TestGraphBuilderInsertWithUnion:
    """Tests for INSERT statements containing UNION queries in graph building."""

    def test_insert_union_creates_qualified_nodes(self, tmp_path):
        """INSERT with UNION should create nodes qualified with target table."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            INSERT INTO db.output
            SELECT id, name FROM db.table_a
            UNION
            SELECT id, name FROM db.table_b
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Check nodes are properly qualified with target table
        node_ids = {node.identifier for node in graph.nodes}
        assert "db.output.id" in node_ids
        assert "db.output.name" in node_ids
        # Source nodes should also exist
        assert "db.table_a.id" in node_ids
        assert "db.table_b.id" in node_ids

    def test_cross_query_lineage_with_union(self, tmp_path):
        """Test that lineage chains work across queries with INSERT UNION.

        This is the key test for the bug fix - lineage should flow from
        input tables through the intermediate table to the final output.
        """
        from sqlglider.graph.query import GraphQuerier

        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            INSERT OVERWRITE TABLE db.output_table_1
            SELECT DISTINCT
                a.id,
                trim(concat(coalesce(a.address_one, ""), " ", coalesce(a.address_two, ""))) AS full_address
            FROM db.input_a AS a
            UNION
            SELECT DISTINCT
                b.id,
                trim(concat(coalesce(b.address_part_a, ""), " ", coalesce(b.address_part_b, ""))) AS full_address
            FROM db.input_b AS b;

            INSERT OVERWRITE TABLE db.output_table_2
            SELECT
                o.id,
                o.full_address AS address
            FROM db.output_table_1 AS o;
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Verify intermediate nodes exist and are qualified
        node_ids = {node.identifier for node in graph.nodes}
        assert "db.output_table_1.full_address" in node_ids
        assert "db.output_table_2.address" in node_ids

        # Query downstream from input
        querier = GraphQuerier(graph)
        downstream = querier.find_downstream("db.input_a.address_one")

        # Should include both intermediate and final output
        downstream_ids = {n.identifier for n in downstream.related_columns}
        assert "db.output_table_1.full_address" in downstream_ids
        assert "db.output_table_2.address" in downstream_ids

    def test_upstream_query_through_union(self, tmp_path):
        """Test upstream query flows through INSERT UNION to source tables."""
        from sqlglider.graph.query import GraphQuerier

        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            INSERT INTO db.intermediate
            SELECT CONCAT(a.first, a.last) AS full_name
            FROM db.source_a AS a
            UNION
            SELECT CONCAT(b.first, b.last) AS full_name
            FROM db.source_b AS b;

            INSERT INTO db.final
            SELECT full_name AS name FROM db.intermediate;
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Query upstream from final output
        querier = GraphQuerier(graph)
        upstream = querier.find_upstream("db.final.name")

        upstream_ids = {n.identifier for n in upstream.related_columns}

        # Should include intermediate and original sources from both UNION branches
        assert "db.intermediate.full_name" in upstream_ids
        assert "db.source_a.first" in upstream_ids
        assert "db.source_a.last" in upstream_ids
        assert "db.source_b.first" in upstream_ids
        assert "db.source_b.last" in upstream_ids


class TestGraphBuilderCreateViewWithCTEAndWindowFunction:
    """Tests for CREATE VIEW statements with CTEs and window functions."""

    def test_create_view_with_cte_and_row_number(self, tmp_path):
        """CREATE VIEW with CTE and ROW_NUMBER() OVER (PARTITION BY ...) should work."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            CREATE VIEW my_view AS
            WITH ranked_orders AS (
                SELECT
                    customer_id,
                    order_date,
                    amount,
                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rn
                FROM orders
            )
            SELECT customer_id, order_date, amount
            FROM ranked_orders
            WHERE rn = 1
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Should have nodes created successfully
        assert graph.metadata.total_nodes > 0
        assert graph.metadata.total_edges > 0

        # Check that output columns are qualified with the view name
        node_ids = {node.identifier for node in graph.nodes}
        assert "my_view.customer_id" in node_ids
        assert "my_view.order_date" in node_ids
        assert "my_view.amount" in node_ids

        # Source columns from orders table should exist
        assert "orders.customer_id" in node_ids
        assert "orders.order_date" in node_ids
        assert "orders.amount" in node_ids

    def test_create_view_with_cte_row_number_lineage_tracing(self, tmp_path):
        """Test that lineage correctly traces through CTE with window function."""
        from sqlglider.graph.query import GraphQuerier

        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            CREATE VIEW latest_orders AS
            WITH ranked AS (
                SELECT
                    o.customer_id,
                    o.order_date,
                    o.total_amount,
                    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC) as rn
                FROM sales.orders o
            )
            SELECT customer_id, order_date, total_amount
            FROM ranked
            WHERE rn = 1
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Query upstream from output columns
        querier = GraphQuerier(graph)

        # customer_id should trace back to sales.orders.customer_id
        upstream_customer = querier.find_upstream("latest_orders.customer_id")
        upstream_ids = {n.identifier for n in upstream_customer.related_columns}
        assert "sales.orders.customer_id" in upstream_ids

        # total_amount should trace back to sales.orders.total_amount
        upstream_amount = querier.find_upstream("latest_orders.total_amount")
        upstream_ids = {n.identifier for n in upstream_amount.related_columns}
        assert "sales.orders.total_amount" in upstream_ids

    def test_create_view_multiple_window_functions(self, tmp_path):
        """Test CREATE VIEW with multiple window functions."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            CREATE VIEW customer_rankings AS
            WITH metrics AS (
                SELECT
                    customer_id,
                    total_spend,
                    ROW_NUMBER() OVER (ORDER BY total_spend DESC) as spend_rank,
                    RANK() OVER (PARTITION BY region ORDER BY total_spend DESC) as region_rank,
                    LAG(total_spend) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_spend
                FROM customer_orders
            )
            SELECT customer_id, total_spend, spend_rank, region_rank
            FROM metrics
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        # Should process successfully with multiple window functions
        assert graph.metadata.total_nodes > 0

        node_ids = {node.identifier for node in graph.nodes}
        assert "customer_rankings.customer_id" in node_ids
        assert "customer_rankings.total_spend" in node_ids
        assert "customer_rankings.spend_rank" in node_ids
        assert "customer_rankings.region_rank" in node_ids

    def test_create_view_nested_ctes_with_window(self, tmp_path):
        """Test CREATE VIEW with nested CTEs and window functions."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("""
            CREATE VIEW final_report AS
            WITH base_data AS (
                SELECT customer_id, product_id, quantity, sale_date
                FROM raw_sales
            ),
            ranked_sales AS (
                SELECT
                    customer_id,
                    product_id,
                    quantity,
                    ROW_NUMBER() OVER (
                        PARTITION BY customer_id, product_id
                        ORDER BY sale_date DESC
                    ) as sale_rank
                FROM base_data
            )
            SELECT customer_id, product_id, quantity
            FROM ranked_sales
            WHERE sale_rank = 1
        """)

        builder = GraphBuilder(dialect="spark")
        builder.add_file(sql_file)
        graph = builder.build()

        assert graph.metadata.total_nodes > 0

        node_ids = {node.identifier for node in graph.nodes}
        assert "final_report.customer_id" in node_ids
        assert "final_report.product_id" in node_ids
        assert "final_report.quantity" in node_ids

        # Source should trace to raw_sales
        assert "raw_sales.customer_id" in node_ids
        assert "raw_sales.product_id" in node_ids
        assert "raw_sales.quantity" in node_ids


class TestGraphBuilderCaseInsensitive:
    """Tests for case-insensitive node deduplication during graph build."""

    def test_nodes_deduplicated_across_files_different_case(self, tmp_path):
        """Nodes with different casing should be deduplicated into one node."""
        file1 = tmp_path / "query1.sql"
        file1.write_text("SELECT Customer_ID FROM Orders;")

        file2 = tmp_path / "query2.sql"
        file2.write_text("SELECT customer_id FROM orders;")

        builder = GraphBuilder()
        builder.add_file(file1)
        builder.add_file(file2)
        graph = builder.build()

        # Should have deduplicated nodes regardless of case
        identifiers_lower = [n.identifier.lower() for n in graph.nodes]
        assert identifiers_lower.count("orders.customer_id") == 1

    def test_edges_deduplicated_across_files_different_case(self, tmp_path):
        """Edges with different casing should be deduplicated."""
        file1 = tmp_path / "query1.sql"
        file1.write_text("SELECT Customer_Name FROM Customers;")

        file2 = tmp_path / "query2.sql"
        file2.write_text("SELECT customer_name FROM customers;")

        builder = GraphBuilder()
        builder.add_file(file1)
        builder.add_file(file2)
        graph = builder.build()

        # Edges should be deduplicated
        edge_pairs = [
            (e.source_node.lower(), e.target_node.lower()) for e in graph.edges
        ]
        for pair in edge_pairs:
            assert edge_pairs.count(pair) == 1

    def test_node_index_map_uses_lowercase_keys(self, tmp_path):
        """The internal node index map should use lowercase keys."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT Customer_ID FROM Orders;")

        builder = GraphBuilder()
        builder.add_file(sql_file)

        # All keys in the node index map should be lowercase
        for key in builder.node_index_map:
            assert key == key.lower(), f"Node index map key '{key}' is not lowercase"


class TestResolveSchema:
    """Tests for --resolve-schema two-pass schema resolution."""

    def test_cross_file_star_resolution(self, tmp_path):
        """Schema from file A's CREATE VIEW enables star resolution in file B."""
        file_a = tmp_path / "a_create_view.sql"
        file_a.write_text(
            "CREATE VIEW customer_summary AS "
            "SELECT customer_id, customer_name FROM customers;"
        )
        file_b = tmp_path / "b_use_view.sql"
        file_b.write_text("SELECT * FROM customer_summary;")

        builder = GraphBuilder(resolve_schema=True)
        builder.add_files([file_a, file_b])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        # Star should have been expanded to actual columns
        assert "customer_summary.customer_id" in node_ids
        assert "customer_summary.customer_name" in node_ids
        # Should NOT have a wildcard placeholder
        assert "customer_summary.*" not in node_ids

    def test_cross_file_star_resolution_reverse_order(self, tmp_path):
        """Order doesn't matter — file B processed before file A still works."""
        file_a = tmp_path / "z_create_view.sql"  # sorted last
        file_a.write_text(
            "CREATE VIEW customer_summary AS "
            "SELECT customer_id, customer_name FROM customers;"
        )
        file_b = tmp_path / "a_use_view.sql"  # sorted first
        file_b.write_text("SELECT * FROM customer_summary;")

        builder = GraphBuilder(resolve_schema=True)
        builder.add_files([file_b, file_a])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        assert "customer_summary.customer_id" in node_ids
        assert "customer_summary.customer_name" in node_ids

    def test_without_resolve_schema_star_not_expanded(self, tmp_path):
        """Without --resolve-schema, cross-file stars are NOT resolved."""
        file_a = tmp_path / "a_create_view.sql"
        file_a.write_text(
            "CREATE VIEW customer_summary AS "
            "SELECT customer_id, customer_name FROM customers;"
        )
        file_b = tmp_path / "b_use_view.sql"
        file_b.write_text("SELECT * FROM customer_summary;")

        builder = GraphBuilder(resolve_schema=False)
        builder.add_files([file_a, file_b])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        # Without resolve_schema, file B's star is unresolved — produces '*'
        assert "*" in node_ids

    def test_no_star_with_resolve_schema_passes(self, tmp_path):
        """--no-star + --resolve-schema succeeds when schema is available."""
        file_a = tmp_path / "a_create_view.sql"
        file_a.write_text(
            "CREATE VIEW customer_summary AS "
            "SELECT customer_id, customer_name FROM customers;"
        )
        file_b = tmp_path / "b_use_view.sql"
        file_b.write_text("SELECT * FROM customer_summary;")

        builder = GraphBuilder(resolve_schema=True, no_star=True)
        builder.add_files([file_a, file_b])
        graph = builder.build()

        assert graph.metadata.total_nodes > 0

    def test_resolve_schema_accumulated_across_files(self, tmp_path):
        """Schema accumulates across multiple files."""
        file1 = tmp_path / "a.sql"
        file1.write_text("CREATE VIEW v1 AS SELECT id, name FROM t1;")
        file2 = tmp_path / "b.sql"
        file2.write_text("CREATE VIEW v2 AS SELECT code, value FROM t2;")
        file3 = tmp_path / "c.sql"
        file3.write_text("SELECT * FROM v1; SELECT * FROM v2;")

        builder = GraphBuilder(resolve_schema=True)
        builder.add_files([file1, file2, file3])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        assert "v1.id" in node_ids
        assert "v1.name" in node_ids
        assert "v2.code" in node_ids
        assert "v2.value" in node_ids

    @patch("sqlglider.catalog.get_catalog")
    def test_catalog_fills_missing_schema(self, mock_get_catalog, tmp_path):
        """Catalog provides DDL for tables not found in files."""
        # File uses SELECT * from a table not defined in any file
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM remote_table;")

        # Mock catalog returns DDL for remote_table
        mock_catalog = MagicMock()
        mock_catalog.get_ddl_batch.return_value = {
            "remote_table": "CREATE TABLE remote_table (id INT, value STRING)"
        }
        mock_get_catalog.return_value = mock_catalog

        builder = GraphBuilder(
            resolve_schema=True,
            catalog_type="mock_catalog",
        )
        builder.add_files([sql_file])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        assert "remote_table.id" in node_ids
        assert "remote_table.value" in node_ids
        mock_catalog.get_ddl_batch.assert_called_once()

    @patch("sqlglider.catalog.get_catalog")
    def test_catalog_does_not_override_file_schema(self, mock_get_catalog, tmp_path):
        """File-derived schema takes priority over catalog (fill gaps only)."""
        file_a = tmp_path / "a.sql"
        file_a.write_text("CREATE VIEW my_view AS SELECT id, name FROM source;")
        file_b = tmp_path / "b.sql"
        file_b.write_text("SELECT * FROM my_view;")

        # Catalog would return different columns — should not be used
        mock_catalog = MagicMock()
        mock_catalog.get_ddl_batch.return_value = {}
        mock_get_catalog.return_value = mock_catalog

        builder = GraphBuilder(
            resolve_schema=True,
            catalog_type="mock_catalog",
        )
        builder.add_files([file_a, file_b])
        graph = builder.build()

        node_ids = {n.identifier for n in graph.nodes}
        # my_view schema from file, not catalog
        assert "my_view.id" in node_ids
        assert "my_view.name" in node_ids

    @patch("sqlglider.catalog.get_catalog")
    def test_catalog_error_handling(self, mock_get_catalog, tmp_path):
        """Catalog errors for individual tables don't fail the build."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM missing_table;")

        mock_catalog = MagicMock()
        mock_catalog.get_ddl_batch.return_value = {
            "missing_table": "ERROR: Table not found"
        }
        mock_get_catalog.return_value = mock_catalog

        builder = GraphBuilder(
            resolve_schema=True,
            catalog_type="mock_catalog",
        )
        # Should not raise — errors are handled gracefully
        builder.add_files([sql_file])
        graph = builder.build()
        assert graph is not None


class TestResolvedSchemaProperty:
    """Tests for the resolved_schema property."""

    def test_empty_without_resolve(self, tmp_path):
        """resolved_schema is empty when resolve_schema is not used."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT id FROM users;")
        builder = GraphBuilder()
        builder.add_file(sql_file)
        assert builder.resolved_schema == {}

    def test_populated_with_resolve_from_dql(self, tmp_path):
        """resolved_schema infers table schemas from DQL column references."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT c.id, c.name FROM customers c;")
        builder = GraphBuilder(resolve_schema=True)
        builder.add_files([sql_file])
        schema = builder.resolved_schema
        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]

    def test_populated_with_resolve_from_create(self, tmp_path):
        """resolved_schema extracts from CREATE VIEW AS SELECT."""
        sql_file = tmp_path / "ddl.sql"
        sql_file.write_text(
            "CREATE VIEW customers AS SELECT id, name FROM raw_customers;"
        )
        builder = GraphBuilder(resolve_schema=True)
        builder.add_files([sql_file])
        schema = builder.resolved_schema
        assert "customers" in schema
        assert "id" in schema["customers"]

    def test_returns_copy(self, tmp_path):
        """resolved_schema returns a copy, not the internal dict."""
        builder = GraphBuilder()
        schema = builder.resolved_schema
        schema["injected"] = {"col": "UNKNOWN"}
        assert "injected" not in builder.resolved_schema


class TestExtractSchemas:
    """Tests for the public extract_schemas method."""

    def test_extract_schemas_returns_schema(self, tmp_path):
        """extract_schemas returns inferred schema from files."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT c.id, c.name FROM customers c;")
        builder = GraphBuilder(resolve_schema=True)
        schema = builder.extract_schemas([sql_file])
        assert "customers" in schema
        assert "id" in schema["customers"]
        assert "name" in schema["customers"]

    def test_extract_schemas_before_add_files(self, tmp_path):
        """Calling extract_schemas before add_files avoids duplicate extraction."""
        schema_file = tmp_path / "schema.sql"
        schema_file.write_text("SELECT c.id, c.name FROM customers c;")
        query_file = tmp_path / "query.sql"
        query_file.write_text("SELECT * FROM customers;")

        builder = GraphBuilder(resolve_schema=True)
        schema = builder.extract_schemas([schema_file, query_file])
        assert "customers" in schema

        # add_files should skip Pass 1 since schema is already resolved
        builder.add_files([query_file])
        graph = builder.build()
        assert graph is not None

        # Schema should still be the same
        assert builder.resolved_schema == schema

    def test_extract_schemas_populates_resolved_schema(self, tmp_path):
        """extract_schemas populates the resolved_schema property."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT u.id, u.email FROM users u;")
        builder = GraphBuilder(resolve_schema=True)
        assert builder.resolved_schema == {}
        builder.extract_schemas([sql_file])
        assert "users" in builder.resolved_schema
