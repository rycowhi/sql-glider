"""Tests for variable loading utilities."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from sqlglider.templating.variables import (
    load_all_variables,
    load_env_variables,
    load_variables_file,
    merge_variables,
    parse_cli_variables,
)


class TestLoadVariablesFile:
    """Tests for load_variables_file function."""

    def test_load_json_file(self):
        """Test loading variables from JSON file."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.json"
            vars_file.write_text('{"schema": "analytics", "table": "users"}')

            result = load_variables_file(vars_file)
            assert result == {"schema": "analytics", "table": "users"}

    def test_load_json_file_with_types(self):
        """Test loading JSON with different value types."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.json"
            vars_file.write_text(
                '{"string": "value", "number": 42, "float": 3.14, "bool": true}'
            )

            result = load_variables_file(vars_file)
            assert result["string"] == "value"
            assert result["number"] == 42
            assert result["float"] == 3.14
            assert result["bool"] is True

    def test_load_json_file_not_found(self):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_variables_file(Path("/nonexistent/vars.json"))

    def test_load_invalid_json(self):
        """Test that invalid JSON raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.json"
            vars_file.write_text("{invalid json}")

            with pytest.raises(ValueError) as exc_info:
                load_variables_file(vars_file)
            assert "json" in str(exc_info.value).lower()

    def test_load_json_non_object(self):
        """Test that non-object JSON raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.json"
            vars_file.write_text('["array", "not", "object"]')

            with pytest.raises(ValueError) as exc_info:
                load_variables_file(vars_file)
            assert "object" in str(exc_info.value).lower()

    def test_load_unsupported_format(self):
        """Test that unsupported file format raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.txt"
            vars_file.write_text("key=value")

            with pytest.raises(ValueError) as exc_info:
                load_variables_file(vars_file)
            assert "unsupported" in str(exc_info.value).lower()

    def test_load_empty_json(self):
        """Test loading empty JSON object."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.json"
            vars_file.write_text("{}")

            result = load_variables_file(vars_file)
            assert result == {}


class TestLoadYamlFile:
    """Tests for loading YAML files."""

    def test_load_yaml_file(self):
        """Test loading variables from YAML file."""
        pytest.importorskip("yaml")

        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.yaml"
            vars_file.write_text("schema: analytics\ntable: users")

            result = load_variables_file(vars_file)
            assert result == {"schema": "analytics", "table": "users"}

    def test_load_yml_extension(self):
        """Test loading with .yml extension."""
        pytest.importorskip("yaml")

        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.yml"
            vars_file.write_text("key: value")

            result = load_variables_file(vars_file)
            assert result == {"key": "value"}

    def test_load_empty_yaml(self):
        """Test loading empty YAML file."""
        pytest.importorskip("yaml")

        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.yaml"
            vars_file.write_text("")

            result = load_variables_file(vars_file)
            assert result == {}


class TestLoadTomlFile:
    """Tests for loading TOML files."""

    def test_load_toml_file(self):
        """Test loading variables from TOML file."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text('schema = "analytics"\ntable = "users"')

            result = load_variables_file(vars_file)
            assert result == {"schema": "analytics", "table": "users"}

    def test_load_toml_with_types(self):
        """Test loading TOML with different value types."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text(
                'string = "value"\nnumber = 42\nfloat_val = 3.14\nbool_val = true'
            )

            result = load_variables_file(vars_file)
            assert result["string"] == "value"
            assert result["number"] == 42
            assert result["float_val"] == 3.14
            assert result["bool_val"] is True

    def test_load_toml_nested_tables(self):
        """Test loading TOML with nested tables."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text('[database]\nhost = "localhost"\nport = 5432')

            result = load_variables_file(vars_file)
            assert result == {"database": {"host": "localhost", "port": 5432}}

    def test_load_invalid_toml(self):
        """Test that invalid TOML raises ValueError."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text("invalid = [unclosed")

            with pytest.raises(ValueError) as exc_info:
                load_variables_file(vars_file)
            assert "toml" in str(exc_info.value).lower()

    def test_load_empty_toml(self):
        """Test loading empty TOML file."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text("")

            result = load_variables_file(vars_file)
            assert result == {}

    def test_load_toml_with_arrays(self):
        """Test loading TOML with arrays."""
        with TemporaryDirectory() as tmpdir:
            vars_file = Path(tmpdir) / "vars.toml"
            vars_file.write_text('columns = ["id", "name", "email"]')

            result = load_variables_file(vars_file)
            assert result == {"columns": ["id", "name", "email"]}


class TestParseCliVariables:
    """Tests for parse_cli_variables function."""

    def test_single_variable(self):
        """Test parsing single variable."""
        result = parse_cli_variables(["schema=analytics"])
        assert result == {"schema": "analytics"}

    def test_multiple_variables(self):
        """Test parsing multiple variables."""
        result = parse_cli_variables(["schema=analytics", "table=users"])
        assert result == {"schema": "analytics", "table": "users"}

    def test_integer_inference(self):
        """Test that integers are inferred."""
        result = parse_cli_variables(["limit=100"])
        assert result["limit"] == 100
        assert isinstance(result["limit"], int)

    def test_float_inference(self):
        """Test that floats are inferred."""
        result = parse_cli_variables(["threshold=0.95"])
        assert result["threshold"] == 0.95
        assert isinstance(result["threshold"], float)

    def test_boolean_true_inference(self):
        """Test that 'true' is inferred as boolean True."""
        result = parse_cli_variables(["enabled=true"])
        assert result["enabled"] is True

    def test_boolean_false_inference(self):
        """Test that 'false' is inferred as boolean False."""
        result = parse_cli_variables(["enabled=false"])
        assert result["enabled"] is False

    def test_boolean_case_insensitive(self):
        """Test that boolean inference is case-insensitive."""
        result = parse_cli_variables(["a=TRUE", "b=False", "c=TrUe"])
        assert result["a"] is True
        assert result["b"] is False
        assert result["c"] is True

    def test_value_with_equals_sign(self):
        """Test value containing equals sign."""
        result = parse_cli_variables(["expr=a=b"])
        assert result["expr"] == "a=b"

    def test_empty_list(self):
        """Test with empty list."""
        result = parse_cli_variables([])
        assert result == {}

    def test_none_input(self):
        """Test with None input."""
        result = parse_cli_variables(None)
        assert result == {}

    def test_missing_equals_raises_error(self):
        """Test that missing equals sign raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_cli_variables(["invalid_format"])
        assert "key=value" in str(exc_info.value).lower()

    def test_empty_key_raises_error(self):
        """Test that empty key raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_cli_variables(["=value"])
        assert "empty" in str(exc_info.value).lower()

    def test_whitespace_stripped(self):
        """Test that whitespace is stripped."""
        result = parse_cli_variables(["  key  =  value  "])
        assert result == {"key": "value"}


class TestLoadEnvVariables:
    """Tests for load_env_variables function."""

    def test_load_env_variables(self, monkeypatch):
        """Test loading variables from environment."""
        monkeypatch.setenv("SQLGLIDER_VAR_SCHEMA", "analytics")
        monkeypatch.setenv("SQLGLIDER_VAR_TABLE", "users")
        result = load_env_variables()
        assert result["schema"] == "analytics"
        assert result["table"] == "users"

    def test_key_is_lowercased(self, monkeypatch):
        """Test that environment variable names are lowercased."""
        monkeypatch.setenv("SQLGLIDER_VAR_UPPER_CASE", "value")
        result = load_env_variables()
        assert "upper_case" in result

    def test_type_inference(self, monkeypatch):
        """Test that types are inferred from env values."""
        monkeypatch.setenv("SQLGLIDER_VAR_NUM", "42")
        monkeypatch.setenv("SQLGLIDER_VAR_BOOL", "true")
        result = load_env_variables()
        assert result["num"] == 42
        assert result["bool"] is True

    def test_custom_prefix(self, monkeypatch):
        """Test with custom prefix."""
        monkeypatch.setenv("CUSTOM_VAR_KEY", "value")
        result = load_env_variables(prefix="CUSTOM_VAR_")
        assert result["key"] == "value"

    def test_ignores_non_matching_vars(self, monkeypatch):
        """Test that non-matching env vars are ignored."""
        monkeypatch.setenv("OTHER_VAR", "ignored")
        result = load_env_variables()
        assert "other_var" not in result


class TestMergeVariables:
    """Tests for merge_variables function."""

    def test_merge_two_sources(self):
        """Test merging two variable sources."""
        source1 = {"a": 1, "b": 2}
        source2 = {"b": 3, "c": 4}
        result = merge_variables(source1, source2)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_later_source_wins(self):
        """Test that later sources override earlier ones."""
        low = {"key": "low"}
        medium = {"key": "medium"}
        high = {"key": "high"}
        result = merge_variables(low, medium, high)
        assert result["key"] == "high"

    def test_merge_with_none(self):
        """Test merging with None source."""
        source = {"key": "value"}
        result = merge_variables(None, source, None)
        assert result == {"key": "value"}

    def test_merge_empty_sources(self):
        """Test merging empty sources."""
        result = merge_variables({}, {}, {})
        assert result == {}

    def test_merge_no_sources(self):
        """Test merging with no sources."""
        result = merge_variables()
        assert result == {}

    def test_merge_single_source(self):
        """Test merging single source."""
        source = {"key": "value"}
        result = merge_variables(source)
        assert result == {"key": "value"}


class TestLoadAllVariables:
    """Tests for load_all_variables function."""

    def test_cli_vars_highest_priority(self, monkeypatch):
        """Test that CLI vars have highest priority."""
        monkeypatch.setenv("SQLGLIDER_VAR_KEY", "env")
        result = load_all_variables(
            cli_vars=["key=cli"],
            config_vars={"key": "config"},
            use_env=True,
        )
        assert result["key"] == "cli"

    def test_file_over_config(self, tmp_path):
        """Test that file vars override config vars."""
        vars_file = tmp_path / "vars.json"
        vars_file.write_text('{"key": "file"}')

        result = load_all_variables(
            vars_file=vars_file,
            config_vars={"key": "config"},
            use_env=False,
        )
        assert result["key"] == "file"

    def test_config_over_env(self, monkeypatch):
        """Test that config vars override env vars."""
        monkeypatch.setenv("SQLGLIDER_VAR_KEY", "env")
        result = load_all_variables(
            config_vars={"key": "config"},
            use_env=True,
        )
        assert result["key"] == "config"

    def test_env_disabled(self, monkeypatch):
        """Test that env vars are not loaded when disabled."""
        monkeypatch.setenv("SQLGLIDER_VAR_KEY", "env")
        result = load_all_variables(use_env=False)
        assert "key" not in result

    def test_all_sources_combined(self, monkeypatch, tmp_path):
        """Test combining all sources."""
        monkeypatch.setenv("SQLGLIDER_VAR_ENV_ONLY", "from_env")
        vars_file = tmp_path / "vars.json"
        vars_file.write_text('{"file_only": "from_file", "shared": "file"}')

        result = load_all_variables(
            cli_vars=["cli_only=from_cli", "shared=cli"],
            vars_file=vars_file,
            config_vars={"config_only": "from_config", "shared": "config"},
            use_env=True,
        )

        assert result["env_only"] == "from_env"
        assert result["config_only"] == "from_config"
        assert result["file_only"] == "from_file"
        assert result["cli_only"] == "from_cli"
        assert result["shared"] == "cli"  # CLI wins
