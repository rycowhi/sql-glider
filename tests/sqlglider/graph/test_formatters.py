"""Tests for graph schema formatters."""

import pytest

from sqlglider.graph.formatters import (
    format_schema,
    format_schema_csv,
    format_schema_json,
    format_schema_text,
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
