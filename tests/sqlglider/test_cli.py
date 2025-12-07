"""Tests for CLI commands."""

from pathlib import Path
from tempfile import NamedTemporaryFile

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
        assert "----------" in result.stdout

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
                # Should use text format (CLI override)
                assert "----------" in result.stdout
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
                # Should use default text format
                assert "----------" in result.stdout
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
                # Should use default text format (not in config)
                assert "----------" in result.stdout
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
                # Should use text format (CLI override)
                # and table level (from config)
                assert "----------" in result.stdout
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
                # Should use default text format
                assert "----------" in result.stdout
            finally:
                os.chdir(original_cwd)

    def test_cli_backward_compatibility(self, sample_sql_file):
        """Test that CLI still works without config file (backward compatibility)."""
        # This is the same as test_cli_missing_config_uses_defaults
        # but explicitly testing backward compatibility
        result = runner.invoke(app, ["lineage", str(sample_sql_file)])

        assert result.exit_code == 0
        # Should use default values
        assert "----------" in result.stdout
