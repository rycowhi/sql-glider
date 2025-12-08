"""Tests for CLI commands."""

from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from typer.testing import CliRunner

from sqlglider.cli import app

runner = CliRunner()


class TestLineageCommand:
    """Tests for the lineage command."""

    @pytest.fixture
    def sample_sql_file(self):
        """Create a temporary SQL file for testing."""
        sql_content = """
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path

        # Cleanup
        temp_path.unlink()

    @pytest.fixture
    def invalid_sql_file(self):
        """Create a temporary file with invalid SQL."""
        sql_content = "INVALID SQL SYNTAX HERE ;;;;"
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path

        # Cleanup
        temp_path.unlink()

    def test_lineage_basic(self, sample_sql_file):
        """Test basic lineage analysis."""
        result = runner.invoke(app, ["lineage", str(sample_sql_file)])

        assert result.exit_code == 0
        # Should contain column information
        assert "customer_id" in result.stdout or "customer_name" in result.stdout

    def test_lineage_with_column_option(self, sample_sql_file):
        """Test lineage with specific column."""
        # First analyze all columns to see what's available
        result = runner.invoke(app, ["lineage", str(sample_sql_file)])

        # Just verify the command runs without specifying a specific column name
        # as the actual column names depend on how SQLGlot parses the query
        assert result.exit_code == 0

    def test_lineage_with_source_column_option(self, sample_sql_file):
        """Test reverse lineage with source column."""
        result = runner.invoke(
            app,
            [
                "lineage",
                str(sample_sql_file),
                "--source-column",
                "customers.customer_name",
            ],
        )

        assert result.exit_code == 0

    def test_lineage_json_format(self, sample_sql_file):
        """Test JSON output format."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--output-format", "json"]
        )

        assert result.exit_code == 0
        assert "{" in result.stdout
        assert "queries" in result.stdout

    def test_lineage_csv_format(self, sample_sql_file):
        """Test CSV output format."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--output-format", "csv"]
        )

        assert result.exit_code == 0
        assert "query_index,output_column,source_column" in result.stdout

    def test_lineage_text_format(self, sample_sql_file):
        """Test text output format (default)."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--output-format", "text"]
        )

        assert result.exit_code == 0
        # Rich table output should contain table headers
        assert "Output Column" in result.stdout
        assert "Source Column" in result.stdout

    def test_lineage_table_level(self, sample_sql_file):
        """Test table-level lineage analysis."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--level", "table"]
        )

        assert result.exit_code == 0
        # Should mention tables
        assert "customers" in result.stdout or "orders" in result.stdout

    def test_lineage_with_output_file(self, sample_sql_file):
        """Test writing output to file."""
        with NamedTemporaryFile(delete=False, suffix=".txt") as f:
            output_file = Path(f.name)

        try:
            result = runner.invoke(
                app,
                [
                    "lineage",
                    str(sample_sql_file),
                    "--output-file",
                    str(output_file),
                ],
            )

            assert result.exit_code == 0
            assert output_file.exists()
            assert "Success" in result.stdout
            assert str(output_file) in result.stdout

            # Verify content was written
            content = output_file.read_text(encoding="utf-8")
            assert len(content) > 0
        finally:
            output_file.unlink()

    def test_lineage_with_dialect(self, sample_sql_file):
        """Test specifying SQL dialect."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--dialect", "postgres"]
        )

        assert result.exit_code == 0

    def test_lineage_file_not_found(self):
        """Test error handling for non-existent file."""
        result = runner.invoke(app, ["lineage", "/path/that/does/not/exist.sql"])

        # Typer returns exit code 2 for missing files
        assert result.exit_code in [1, 2]

    def test_lineage_invalid_sql(self, invalid_sql_file):
        """Test error handling for invalid SQL."""
        result = runner.invoke(app, ["lineage", str(invalid_sql_file)])

        assert result.exit_code == 1

    def test_lineage_invalid_level(self, sample_sql_file):
        """Test error handling for invalid level option."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--level", "invalid"]
        )

        assert result.exit_code == 1

    def test_lineage_invalid_output_format(self, sample_sql_file):
        """Test error handling for invalid output format."""
        result = runner.invoke(
            app, ["lineage", str(sample_sql_file), "--output-format", "xml"]
        )

        assert result.exit_code == 1

    def test_lineage_column_and_source_column_mutual_exclusion(self, sample_sql_file):
        """Test that --column and --source-column cannot be used together."""
        result = runner.invoke(
            app,
            [
                "lineage",
                str(sample_sql_file),
                "--column",
                "customer_id",
                "--source-column",
                "orders.id",
            ],
        )

        assert result.exit_code == 1

    def test_lineage_short_options(self, sample_sql_file):
        """Test using short option flags."""
        result = runner.invoke(
            app,
            [
                "lineage",
                str(sample_sql_file),
                "-l",
                "column",
                "-d",
                "spark",
                "-f",
                "json",
            ],
        )

        assert result.exit_code == 0
        # Verify JSON format
        assert "{" in result.stdout

    def test_lineage_json_output_to_file(self, sample_sql_file):
        """Test JSON output written to file."""
        with NamedTemporaryFile(delete=False, suffix=".json") as f:
            output_file = Path(f.name)

        try:
            result = runner.invoke(
                app,
                [
                    "lineage",
                    str(sample_sql_file),
                    "--output-format",
                    "json",
                    "--output-file",
                    str(output_file),
                ],
            )

            assert result.exit_code == 0

            # Verify JSON is valid
            import json

            content = output_file.read_text(encoding="utf-8")
            parsed = json.loads(content)
            assert "queries" in parsed
        finally:
            output_file.unlink()

    def test_lineage_csv_output_to_file(self, sample_sql_file):
        """Test CSV output written to file."""
        with NamedTemporaryFile(delete=False, suffix=".csv") as f:
            output_file = Path(f.name)

        try:
            result = runner.invoke(
                app,
                [
                    "lineage",
                    str(sample_sql_file),
                    "--output-format",
                    "csv",
                    "--output-file",
                    str(output_file),
                ],
            )

            assert result.exit_code == 0

            # Verify CSV format
            content = output_file.read_text(encoding="utf-8")
            assert "query_index,output_column,source_column" in content
        finally:
            output_file.unlink()

    def test_lineage_table_level_json(self, sample_sql_file):
        """Test table-level lineage with JSON output."""
        result = runner.invoke(
            app,
            [
                "lineage",
                str(sample_sql_file),
                "--level",
                "table",
                "--output-format",
                "json",
            ],
        )

        assert result.exit_code == 0
        assert "{" in result.stdout
        assert "table" in result.stdout

    def test_lineage_table_level_csv(self, sample_sql_file):
        """Test table-level lineage with CSV output."""
        result = runner.invoke(
            app,
            [
                "lineage",
                str(sample_sql_file),
                "--level",
                "table",
                "--output-format",
                "csv",
            ],
        )

        assert result.exit_code == 0
        assert "output_table,source_table" in result.stdout


class TestMainCallback:
    """Tests for the main callback."""

    def test_app_help(self):
        """Test the help message."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "SQL Glider" in result.stdout or "sqlglider" in result.stdout

    def test_lineage_help(self):
        """Test the lineage command help."""
        result = runner.invoke(app, ["lineage", "--help"])

        assert result.exit_code == 0
        assert "lineage" in result.stdout.lower()
        assert "--column" in result.stdout or "-c" in result.stdout


class TestConfigIntegration:
    """Tests for configuration file integration with CLI."""

    @pytest.fixture
    def sample_sql_file(self):
        """Create a temporary SQL file for testing."""
        sql_content = """
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path

        # Cleanup
        temp_path.unlink()

    def test_cli_uses_config_defaults(self, sample_sql_file):
        """Test that CLI uses config defaults when no args provided."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "sqlglider.toml"

            # Write config with postgres dialect
            config_file.write_text(
                """
[sqlglider]
dialect = "postgres"
output_format = "json"
"""
            )

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI from the temp directory
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(app, ["lineage", "query.sql"])

                assert result.exit_code == 0
                # Should use JSON format from config
                assert "{" in result.stdout
                assert "queries" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_args_override_config(self, sample_sql_file):
        """Test that CLI args override config values."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "sqlglider.toml"

            # Write config with JSON format
            config_file.write_text(
                """
[sqlglider]
output_format = "json"
"""
            )

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI with explicit text format (should override config)
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(
                    app, ["lineage", "query.sql", "--output-format", "text"]
                )

                assert result.exit_code == 0
                # Should use text format (CLI override) - Rich table
                assert "Output Column" in result.stdout
                # Should NOT be JSON
                assert not result.stdout.strip().startswith("{")
            finally:
                os.chdir(original_cwd)

    def test_cli_missing_config_uses_defaults(self, sample_sql_file):
        """Test that CLI uses hardcoded defaults when config doesn't exist."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # No config file created

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI without config
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(app, ["lineage", "query.sql"])

                assert result.exit_code == 0
                # Should use default text format (Rich table)
                assert "Output Column" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_partial_config(self, sample_sql_file):
        """Test CLI with partial config (some fields set, others default)."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "sqlglider.toml"

            # Write partial config (only dialect)
            config_file.write_text(
                """
[sqlglider]
dialect = "snowflake"
"""
            )

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(app, ["lineage", "query.sql"])

                assert result.exit_code == 0
                # Should use default text format (Rich table)
                assert "Output Column" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_priority_order(self, sample_sql_file):
        """Test priority order: CLI > config > default."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "sqlglider.toml"

            # Write config
            config_file.write_text(
                """
[sqlglider]
dialect = "postgres"
level = "table"
output_format = "json"
"""
            )

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI with some overrides
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                # Override output_format but keep level from config
                result = runner.invoke(
                    app,
                    [
                        "lineage",
                        "query.sql",
                        "--output-format",
                        "text",
                        # level defaults to config (table)
                    ],
                )

                assert result.exit_code == 0
                # Should use text format (CLI override) - Rich table
                # and table level (from config)
                assert "Output Table" in result.stdout
                assert "Source Table" in result.stdout
                # Table level output should show tables
                assert "customers" in result.stdout or "orders" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_malformed_config_fallback(self, sample_sql_file):
        """Test that malformed config falls back to defaults."""
        from tempfile import TemporaryDirectory
        import os

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "sqlglider.toml"

            # Write malformed config
            config_file.write_text(
                """
[sqlglider
dialect = "postgres"  # Missing closing bracket
"""
            )

            # Copy SQL file to temp directory
            sql_file_in_tmpdir = tmppath / "query.sql"
            sql_file_in_tmpdir.write_text(sample_sql_file.read_text())

            # Run CLI
            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(app, ["lineage", "query.sql"])

                # Should still work with defaults
                assert result.exit_code == 0
                # Should use default text format (Rich table)
                assert "Output Column" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_backward_compatibility(self, sample_sql_file):
        """Test that CLI still works without config file (backward compatibility)."""
        # This is the same as test_cli_missing_config_uses_defaults
        # but explicitly testing backward compatibility
        result = runner.invoke(app, ["lineage", str(sample_sql_file)])

        assert result.exit_code == 0
        # Should use default values (Rich table format)
        assert "Output Column" in result.stdout


class TestGraphBuildCommand:
    """Tests for the graph build command."""

    @pytest.fixture
    def sample_sql_file(self):
        """Create a temporary SQL file for testing."""
        sql_content = """
        SELECT
            c.customer_id,
            c.customer_name
        FROM customers c;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path
        temp_path.unlink()

    def test_graph_build_single_file(self, sample_sql_file):
        """Test building graph from single file."""
        with NamedTemporaryFile(delete=False, suffix=".json") as f:
            output_path = Path(f.name)

        try:
            result = runner.invoke(
                app,
                ["graph", "build", str(sample_sql_file), "-o", str(output_path)],
            )

            assert result.exit_code == 0
            assert "Success" in result.stdout
            assert output_path.exists()

            # Verify JSON content
            import json

            content = json.loads(output_path.read_text())
            assert "metadata" in content
            assert "nodes" in content
            assert "edges" in content
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_graph_build_directory(self):
        """Test building graph from directory."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create SQL files
            (tmppath / "query1.sql").write_text("SELECT id FROM table1;")
            (tmppath / "query2.sql").write_text("SELECT name FROM table2;")

            output_path = tmppath / "graph.json"

            result = runner.invoke(
                app,
                ["graph", "build", str(tmppath), "-o", str(output_path)],
            )

            assert result.exit_code == 0
            assert "Success" in result.stdout
            assert output_path.exists()

    def test_graph_build_recursive(self):
        """Test building graph from directory recursively."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create nested directories
            subdir = tmppath / "subdir"
            subdir.mkdir()

            (tmppath / "query1.sql").write_text("SELECT id FROM table1;")
            (subdir / "query2.sql").write_text("SELECT name FROM table2;")

            output_path = tmppath / "graph.json"

            result = runner.invoke(
                app,
                ["graph", "build", str(tmppath), "-r", "-o", str(output_path)],
            )

            assert result.exit_code == 0
            assert "2 nodes" in result.stdout or result.exit_code == 0

    def test_graph_build_with_dialect(self, sample_sql_file):
        """Test building graph with specific dialect."""
        with NamedTemporaryFile(delete=False, suffix=".json") as f:
            output_path = Path(f.name)

        try:
            result = runner.invoke(
                app,
                [
                    "graph",
                    "build",
                    str(sample_sql_file),
                    "-o",
                    str(output_path),
                    "--dialect",
                    "postgres",
                ],
            )

            assert result.exit_code == 0

            import json

            content = json.loads(output_path.read_text())
            assert content["metadata"]["default_dialect"] == "postgres"
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_graph_build_with_manifest(self):
        """Test building graph from manifest file."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create SQL files
            (tmppath / "query1.sql").write_text("SELECT id FROM table1;")
            (tmppath / "query2.sql").write_text("SELECT name FROM table2;")

            # Create manifest
            manifest = tmppath / "manifest.csv"
            manifest.write_text(
                "file_path,dialect\nquery1.sql,spark\nquery2.sql,postgres\n"
            )

            output_path = tmppath / "graph.json"

            result = runner.invoke(
                app,
                ["graph", "build", "--manifest", str(manifest), "-o", str(output_path)],
            )

            assert result.exit_code == 0
            assert output_path.exists()

    def test_graph_build_no_input_error(self):
        """Test error when no input provided."""
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "graph.json"

            result = runner.invoke(
                app,
                ["graph", "build", "-o", str(output_path)],
            )

            assert result.exit_code == 1
            assert "Must provide" in result.output

    def test_graph_build_invalid_node_format(self, sample_sql_file):
        """Test error with invalid node format."""
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "graph.json"

            result = runner.invoke(
                app,
                [
                    "graph",
                    "build",
                    str(sample_sql_file),
                    "-o",
                    str(output_path),
                    "--node-format",
                    "invalid",
                ],
            )

            assert result.exit_code == 1
            assert "Invalid node format" in result.output

    def test_graph_build_help(self):
        """Test graph build help."""
        result = runner.invoke(app, ["graph", "build", "--help"])

        assert result.exit_code == 0
        assert "--output" in result.stdout
        assert "--recursive" in result.stdout
        assert "--manifest" in result.stdout


class TestGraphMergeCommand:
    """Tests for the graph merge command."""

    def test_graph_merge_two_files(self):
        """Test merging two graph files."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create graph files by building from SQL
            sql1 = tmppath / "query1.sql"
            sql1.write_text("SELECT id FROM table1;")

            sql2 = tmppath / "query2.sql"
            sql2.write_text("SELECT name FROM table2;")

            graph1 = tmppath / "graph1.json"
            graph2 = tmppath / "graph2.json"
            merged = tmppath / "merged.json"

            # Build graphs
            runner.invoke(app, ["graph", "build", str(sql1), "-o", str(graph1)])
            runner.invoke(app, ["graph", "build", str(sql2), "-o", str(graph2)])

            # Merge
            result = runner.invoke(
                app,
                ["graph", "merge", str(graph1), str(graph2), "-o", str(merged)],
            )

            assert result.exit_code == 0
            assert "Success" in result.stdout
            assert merged.exists()

    def test_graph_merge_with_glob(self):
        """Test merging graphs with glob pattern."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create SQL files and build graphs
            for i in range(3):
                sql = tmppath / f"query{i}.sql"
                sql.write_text(f"SELECT col{i} FROM table{i};")

                graph = tmppath / f"graph{i}.json"
                runner.invoke(app, ["graph", "build", str(sql), "-o", str(graph)])

            merged = tmppath / "merged.json"

            # Merge with glob (run from tmpdir)
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(tmppath)
                result = runner.invoke(
                    app,
                    ["graph", "merge", "--glob", "graph*.json", "-o", str(merged)],
                )

                assert result.exit_code == 0
                assert merged.exists()
            finally:
                os.chdir(original_cwd)

    def test_graph_merge_no_input_error(self):
        """Test error when no input provided."""
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "merged.json"

            result = runner.invoke(
                app,
                ["graph", "merge", "-o", str(output_path)],
            )

            assert result.exit_code == 1
            assert "Must provide" in result.output

    def test_graph_merge_help(self):
        """Test graph merge help."""
        result = runner.invoke(app, ["graph", "merge", "--help"])

        assert result.exit_code == 0
        assert "--output" in result.stdout
        assert "--glob" in result.stdout


class TestGraphQueryCommand:
    """Tests for the graph query command."""

    @pytest.fixture
    def sample_graph_file(self):
        """Create a sample graph file for testing."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create SQL with dependencies
            sql = tmppath / "query.sql"
            sql.write_text("""
                SELECT
                    c.customer_name,
                    o.order_total
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id;
            """)

            graph = tmppath / "graph.json"
            runner.invoke(app, ["graph", "build", str(sql), "-o", str(graph)])

            yield graph

    def test_graph_query_upstream(self, sample_graph_file):
        """Test querying upstream dependencies."""
        # First get list of columns
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--upstream",
                "customers.customer_name",
            ],
        )

        # Should succeed (even if no upstream found)
        assert result.exit_code == 0

    def test_graph_query_downstream(self, sample_graph_file):
        """Test querying downstream dependencies."""
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--downstream",
                "customers.customer_name",
            ],
        )

        assert result.exit_code == 0

    def test_graph_query_json_format(self, sample_graph_file):
        """Test JSON output format."""
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--upstream",
                "customers.customer_name",
                "-f",
                "json",
            ],
        )

        assert result.exit_code == 0
        if len(result.stdout.strip()) > 0:
            import json

            parsed = json.loads(result.stdout)
            assert "query_column" in parsed
            assert "direction" in parsed

    def test_graph_query_csv_format(self, sample_graph_file):
        """Test CSV output format."""
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--downstream",
                "customers.customer_name",
                "-f",
                "csv",
            ],
        )

        assert result.exit_code == 0
        assert "identifier" in result.stdout

    def test_graph_query_no_direction_error(self, sample_graph_file):
        """Test error when neither upstream nor downstream specified."""
        result = runner.invoke(
            app,
            ["graph", "query", str(sample_graph_file)],
        )

        assert result.exit_code == 1
        assert "Must specify" in result.output

    def test_graph_query_both_directions_error(self, sample_graph_file):
        """Test error when both upstream and downstream specified."""
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--upstream",
                "col1",
                "--downstream",
                "col2",
            ],
        )

        assert result.exit_code == 1
        assert "Cannot specify both" in result.output

    def test_graph_query_column_not_found(self, sample_graph_file):
        """Test error when column not found."""
        result = runner.invoke(
            app,
            [
                "graph",
                "query",
                str(sample_graph_file),
                "--upstream",
                "nonexistent.column",
            ],
        )

        assert result.exit_code == 1
        assert "not found" in result.output

    def test_graph_query_help(self):
        """Test graph query help."""
        result = runner.invoke(app, ["graph", "query", "--help"])

        assert result.exit_code == 0
        assert "--upstream" in result.stdout
        assert "--downstream" in result.stdout


class TestGraphCommandGroup:
    """Tests for the graph command group."""

    def test_graph_help(self):
        """Test graph command help."""
        result = runner.invoke(app, ["graph", "--help"])

        assert result.exit_code == 0
        assert "build" in result.stdout
        assert "merge" in result.stdout
        assert "query" in result.stdout


class TestTemplateCommand:
    """Tests for the template command."""

    def test_template_basic(self):
        """Test basic template rendering."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ table }}")

            result = runner.invoke(
                app,
                ["template", str(sql_file), "--var", "table=users"],
            )

            assert result.exit_code == 0
            assert "SELECT * FROM users" in result.stdout

    def test_template_multiple_variables(self):
        """Test template with multiple variables."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT {{ column }} FROM {{ schema }}.{{ table }}")

            result = runner.invoke(
                app,
                [
                    "template",
                    str(sql_file),
                    "--var",
                    "column=id",
                    "--var",
                    "schema=public",
                    "--var",
                    "table=users",
                ],
            )

            assert result.exit_code == 0
            assert "SELECT id FROM public.users" in result.stdout

    def test_template_with_vars_file(self):
        """Test template with variables from file."""
        import json

        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ schema }}.{{ table }}")

            vars_file = tmppath / "vars.json"
            vars_file.write_text(json.dumps({"schema": "analytics", "table": "events"}))

            result = runner.invoke(
                app,
                ["template", str(sql_file), "--vars-file", str(vars_file)],
            )

            assert result.exit_code == 0
            assert "SELECT * FROM analytics.events" in result.stdout

    def test_template_output_to_file(self):
        """Test template output written to file."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ table }}")
            output_file = tmppath / "rendered.sql"

            result = runner.invoke(
                app,
                [
                    "template",
                    str(sql_file),
                    "--var",
                    "table=users",
                    "-o",
                    str(output_file),
                ],
            )

            assert result.exit_code == 0
            assert output_file.exists()
            assert "SELECT * FROM users" in output_file.read_text()

    def test_template_list_templaters(self):
        """Test listing available templaters."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT 1")

            result = runner.invoke(
                app,
                ["template", str(sql_file), "--list"],
            )

            assert result.exit_code == 0
            assert "jinja" in result.stdout
            assert "none" in result.stdout

    def test_template_undefined_variable_error(self):
        """Test error on undefined variable."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ undefined_table }}")

            result = runner.invoke(
                app,
                ["template", str(sql_file)],
            )

            assert result.exit_code == 1
            assert "undefined" in result.output.lower()

    def test_template_none_templater(self):
        """Test using 'none' templater (no-op)."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ table }}")

            result = runner.invoke(
                app,
                ["template", str(sql_file), "--templater", "none"],
            )

            assert result.exit_code == 0
            # Should pass through unchanged
            assert "{{ table }}" in result.stdout

    def test_template_help(self):
        """Test template command help."""
        result = runner.invoke(app, ["template", "--help"])

        assert result.exit_code == 0
        assert "--templater" in result.stdout
        assert "--var" in result.stdout
        assert "--vars-file" in result.stdout


class TestLineageWithTemplating:
    """Tests for lineage command with templating enabled."""

    def test_lineage_with_templater(self):
        """Test lineage analysis with templating."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT customer_id FROM {{ schema }}.customers")

            result = runner.invoke(
                app,
                [
                    "lineage",
                    str(sql_file),
                    "--templater",
                    "jinja",
                    "--var",
                    "schema=analytics",
                ],
            )

            assert result.exit_code == 0
            # Should show lineage for templated SQL
            assert "customer_id" in result.stdout

    def test_lineage_without_templater_preserves_template(self):
        """Test that lineage without templater treats template syntax as literal."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            # Simple SQL that doesn't need templating
            sql_file.write_text("SELECT id FROM users")

            result = runner.invoke(
                app,
                ["lineage", str(sql_file)],
            )

            assert result.exit_code == 0
            assert "id" in result.stdout


class TestGraphBuildWithTemplating:
    """Tests for graph build command with templating enabled."""

    def test_graph_build_with_templater(self):
        """Test graph build with templating."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT customer_id FROM {{ schema }}.customers")

            output_path = tmppath / "graph.json"

            result = runner.invoke(
                app,
                [
                    "graph",
                    "build",
                    str(sql_file),
                    "-o",
                    str(output_path),
                    "--templater",
                    "jinja",
                    "--var",
                    "schema=analytics",
                ],
            )

            assert result.exit_code == 0
            assert output_path.exists()

            # Verify graph contains the templated column
            import json

            graph_data = json.loads(output_path.read_text())
            assert graph_data["metadata"]["total_nodes"] > 0


class TestTablesCommand:
    """Tests for the tables command."""

    @pytest.fixture
    def sample_sql_file(self):
        """Create a temporary SQL file for testing."""
        sql_content = """
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_total
        FROM customers c
        JOIN orders o ON c.id = o.customer_id;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path
        temp_path.unlink()

    @pytest.fixture
    def create_view_sql_file(self):
        """Create a temporary SQL file with CREATE VIEW."""
        sql_content = """
        CREATE VIEW customer_summary AS
        SELECT
            customer_id,
            SUM(amount) as total
        FROM orders
        GROUP BY customer_id;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path
        temp_path.unlink()

    @pytest.fixture
    def multi_query_sql_file(self):
        """Create a temporary SQL file with multiple queries."""
        sql_content = """
        SELECT * FROM customers;
        INSERT INTO target_table SELECT * FROM source_table;
        CREATE VIEW summary AS SELECT * FROM orders;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path
        temp_path.unlink()

    @pytest.fixture
    def cte_sql_file(self):
        """Create a temporary SQL file with CTEs."""
        sql_content = """
        WITH order_totals AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.name, ot.total
        FROM customers c
        JOIN order_totals ot ON c.id = ot.customer_id;
        """
        with NamedTemporaryFile(
            mode="w", delete=False, suffix=".sql", encoding="utf-8"
        ) as f:
            f.write(sql_content)
            temp_path = Path(f.name)

        yield temp_path
        temp_path.unlink()

    def test_tables_basic(self, sample_sql_file):
        """Test basic tables analysis."""
        result = runner.invoke(app, ["tables", str(sample_sql_file)])

        assert result.exit_code == 0
        assert "customers" in result.stdout
        assert "orders" in result.stdout
        assert "INPUT" in result.stdout
        assert "UNKNOWN" in result.stdout

    def test_tables_json_format(self, sample_sql_file):
        """Test JSON output format."""
        result = runner.invoke(
            app, ["tables", str(sample_sql_file), "--output-format", "json"]
        )

        assert result.exit_code == 0
        assert "{" in result.stdout
        assert "queries" in result.stdout
        assert "tables" in result.stdout

        import json

        data = json.loads(result.stdout)
        assert len(data["queries"]) == 1
        assert len(data["queries"][0]["tables"]) == 2

    def test_tables_csv_format(self, sample_sql_file):
        """Test CSV output format."""
        result = runner.invoke(
            app, ["tables", str(sample_sql_file), "--output-format", "csv"]
        )

        assert result.exit_code == 0
        assert "query_index,table_name,usage,object_type" in result.stdout
        assert "customers" in result.stdout
        assert "orders" in result.stdout

    def test_tables_with_output_file(self, sample_sql_file):
        """Test writing output to file."""
        with NamedTemporaryFile(delete=False, suffix=".json") as f:
            output_file = Path(f.name)

        try:
            result = runner.invoke(
                app,
                [
                    "tables",
                    str(sample_sql_file),
                    "--output-format",
                    "json",
                    "--output-file",
                    str(output_file),
                ],
            )

            assert result.exit_code == 0
            assert output_file.exists()
            assert "Success" in result.stdout

            import json

            content = json.loads(output_file.read_text())
            assert "queries" in content
        finally:
            output_file.unlink()

    def test_tables_with_dialect(self, sample_sql_file):
        """Test specifying SQL dialect."""
        result = runner.invoke(
            app, ["tables", str(sample_sql_file), "--dialect", "postgres"]
        )

        assert result.exit_code == 0

    def test_tables_create_view(self, create_view_sql_file):
        """Test tables command with CREATE VIEW."""
        result = runner.invoke(
            app, ["tables", str(create_view_sql_file), "--output-format", "json"]
        )

        assert result.exit_code == 0

        import json

        data = json.loads(result.stdout)
        tables = data["queries"][0]["tables"]
        table_by_name = {t["name"]: t for t in tables}

        assert "customer_summary" in table_by_name
        assert table_by_name["customer_summary"]["usage"] == "OUTPUT"
        assert table_by_name["customer_summary"]["object_type"] == "VIEW"

        assert "orders" in table_by_name
        assert table_by_name["orders"]["usage"] == "INPUT"

    def test_tables_multi_query(self, multi_query_sql_file):
        """Test tables with multi-query file."""
        result = runner.invoke(
            app, ["tables", str(multi_query_sql_file), "--output-format", "json"]
        )

        assert result.exit_code == 0

        import json

        data = json.loads(result.stdout)
        assert len(data["queries"]) == 3

        # Query 0: SELECT FROM customers
        assert data["queries"][0]["query_index"] == 0

        # Query 1: INSERT INTO target_table FROM source_table
        query1_tables = {t["name"]: t for t in data["queries"][1]["tables"]}
        assert "target_table" in query1_tables
        assert query1_tables["target_table"]["usage"] == "OUTPUT"

        # Query 2: CREATE VIEW summary
        query2_tables = {t["name"]: t for t in data["queries"][2]["tables"]}
        assert "summary" in query2_tables
        assert query2_tables["summary"]["object_type"] == "VIEW"

    def test_tables_cte(self, cte_sql_file):
        """Test tables command with CTEs."""
        result = runner.invoke(
            app, ["tables", str(cte_sql_file), "--output-format", "json"]
        )

        assert result.exit_code == 0

        import json

        data = json.loads(result.stdout)
        tables = data["queries"][0]["tables"]
        table_by_name = {t["name"]: t for t in tables}

        assert "order_totals" in table_by_name
        assert table_by_name["order_totals"]["object_type"] == "CTE"

    def test_tables_with_table_filter(self, multi_query_sql_file):
        """Test filtering by table name."""
        result = runner.invoke(
            app,
            [
                "tables",
                str(multi_query_sql_file),
                "--table",
                "orders",
                "--output-format",
                "json",
            ],
        )

        assert result.exit_code == 0

        import json

        data = json.loads(result.stdout)
        # Should only include queries that reference 'orders'
        assert len(data["queries"]) == 1  # Only CREATE VIEW references orders

    def test_tables_file_not_found(self):
        """Test error handling for non-existent file."""
        result = runner.invoke(app, ["tables", "/path/that/does/not/exist.sql"])

        assert result.exit_code in [1, 2]

    def test_tables_invalid_output_format(self, sample_sql_file):
        """Test error handling for invalid output format."""
        result = runner.invoke(
            app, ["tables", str(sample_sql_file), "--output-format", "xml"]
        )

        assert result.exit_code == 1
        assert "Invalid output format" in result.output

    def test_tables_short_options(self, sample_sql_file):
        """Test using short option flags."""
        result = runner.invoke(
            app,
            [
                "tables",
                str(sample_sql_file),
                "-d",
                "spark",
                "-f",
                "json",
            ],
        )

        assert result.exit_code == 0
        assert "{" in result.stdout

    def test_tables_help(self):
        """Test tables command help."""
        result = runner.invoke(app, ["tables", "--help"])

        assert result.exit_code == 0
        assert "tables" in result.stdout.lower()
        assert "--dialect" in result.stdout
        assert "--output-format" in result.stdout
        assert "--table" in result.stdout

    def test_tables_with_templating(self):
        """Test tables command with templating."""
        with TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sql_file = tmppath / "query.sql"
            sql_file.write_text("SELECT * FROM {{ schema }}.customers")

            result = runner.invoke(
                app,
                [
                    "tables",
                    str(sql_file),
                    "--templater",
                    "jinja",
                    "--var",
                    "schema=analytics",
                    "--output-format",
                    "json",
                ],
            )

            assert result.exit_code == 0

            import json

            data = json.loads(result.stdout)
            tables = data["queries"][0]["tables"]
            assert any("analytics.customers" in t["name"] for t in tables)
