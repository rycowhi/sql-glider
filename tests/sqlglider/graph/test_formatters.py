"""Tests for graph schema formatters."""

import pytest

from sqlglider.graph.formatters import (
    format_schema,
    format_schema_csv,
    format_schema_json,
    format_schema_text,
    load_schema_file,
    parse_schema_csv,
    parse_schema_json,
    parse_schema_text,
)


@pytest.fixture
def sample_schema():
    return {
        "customers": {"id": "UNKNOWN", "name": "UNKNOWN"},
        "schema.orders": {"order_id": "UNKNOWN", "customer_id": "UNKNOWN"},
    }


class TestFormatSchemaText:
    def test_basic(self, sample_schema):
        result = format_schema_text(sample_schema)
        assert "customers" in result
        assert "  id" in result
        assert "  name" in result
        assert "schema.orders" in result
        assert "  order_id" in result

    def test_tables_sorted(self, sample_schema):
        result = format_schema_text(sample_schema)
        lines = result.strip().split("\n")
        table_lines = [
            line for line in lines if not line.startswith("  ") and line.strip()
        ]
        assert table_lines == ["customers", "schema.orders"]

    def test_empty_schema(self):
        assert format_schema_text({}) == ""


class TestFormatSchemaJson:
    def test_basic(self, sample_schema):
        import json

        result = format_schema_json(sample_schema)
        parsed = json.loads(result)
        assert "customers" in parsed
        assert parsed["customers"]["id"] == "UNKNOWN"

    def test_empty_schema(self):
        import json

        result = format_schema_json({})
        assert json.loads(result) == {}


class TestFormatSchemaCsv:
    def test_basic(self, sample_schema):
        result = format_schema_csv(sample_schema)
        assert "table,column,type" in result
        assert "customers,id,UNKNOWN" in result
        assert "schema.orders,order_id,UNKNOWN" in result

    def test_empty_schema(self):
        result = format_schema_csv({})
        # Should still have header
        assert "table,column,type" in result


class TestFormatSchema:
    def test_text_format(self, sample_schema):
        result = format_schema(sample_schema, "text")
        assert "customers" in result

    def test_json_format(self, sample_schema):
        result = format_schema(sample_schema, "json")
        assert '"customers"' in result

    def test_csv_format(self, sample_schema):
        result = format_schema(sample_schema, "csv")
        assert "table,column,type" in result

    def test_invalid_format(self, sample_schema):
        with pytest.raises(ValueError, match="Invalid schema format"):
            format_schema(sample_schema, "xml")


class TestParseSchemaJson:
    def test_round_trip(self, sample_schema):
        content = format_schema_json(sample_schema)
        parsed = parse_schema_json(content)
        assert parsed == sample_schema

    def test_empty(self):
        assert parse_schema_json("{}") == {}


class TestParseSchemaCsv:
    def test_round_trip(self, sample_schema):
        content = format_schema_csv(sample_schema)
        parsed = parse_schema_csv(content)
        assert parsed == sample_schema

    def test_empty(self):
        parsed = parse_schema_csv("table,column,type\n")
        assert parsed == {}


class TestParseSchemaText:
    def test_round_trip(self, sample_schema):
        content = format_schema_text(sample_schema)
        parsed = parse_schema_text(content)
        assert parsed == sample_schema

    def test_empty(self):
        assert parse_schema_text("") == {}

    def test_single_table(self):
        content = "users\n  id\n  name\n"
        parsed = parse_schema_text(content)
        assert parsed == {"users": {"id": "UNKNOWN", "name": "UNKNOWN"}}


class TestLoadSchemaFile:
    def test_json_extension(self, tmp_path, sample_schema):
        f = tmp_path / "schema.json"
        f.write_text(format_schema_json(sample_schema))
        assert load_schema_file(f) == sample_schema

    def test_csv_extension(self, tmp_path, sample_schema):
        f = tmp_path / "schema.csv"
        f.write_text(format_schema_csv(sample_schema))
        assert load_schema_file(f) == sample_schema

    def test_txt_extension(self, tmp_path, sample_schema):
        f = tmp_path / "schema.txt"
        f.write_text(format_schema_text(sample_schema))
        assert load_schema_file(f) == sample_schema

    def test_no_extension_treated_as_text(self, tmp_path, sample_schema):
        f = tmp_path / "schema"
        f.write_text(format_schema_text(sample_schema))
        assert load_schema_file(f) == sample_schema
