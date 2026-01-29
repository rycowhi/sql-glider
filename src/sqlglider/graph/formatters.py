"""Output formatters for resolved schema data."""

import csv
import json
from io import StringIO
from typing import Dict

SchemaDict = Dict[str, Dict[str, str]]


def format_schema_text(schema: SchemaDict) -> str:
    """Format resolved schema as human-readable text.

    Output format:
        customers
          id
          name

        schema.orders
          order_id
          customer_id

    Args:
        schema: Resolved schema dictionary mapping table names to column dicts.

    Returns:
        Text-formatted string.
    """
    lines: list[str] = []
    for table_name in sorted(schema):
        if lines:
            lines.append("")
        lines.append(table_name)
        for column_name in sorted(schema[table_name]):
            lines.append(f"  {column_name}")
    return "\n".join(lines) + "\n" if lines else ""


def format_schema_json(schema: SchemaDict) -> str:
    """Format resolved schema as JSON.

    Args:
        schema: Resolved schema dictionary mapping table names to column dicts.

    Returns:
        JSON-formatted string.
    """
    sorted_schema = {k: schema[k] for k in sorted(schema)}
    return json.dumps(sorted_schema, indent=2)


def format_schema_csv(schema: SchemaDict) -> str:
    """Format resolved schema as CSV.

    Output format:
        table,column,type
        customers,id,UNKNOWN
        customers,name,UNKNOWN

    Args:
        schema: Resolved schema dictionary mapping table names to column dicts.

    Returns:
        CSV-formatted string.
    """
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["table", "column", "type"])
    for table_name in sorted(schema):
        for column_name in sorted(schema[table_name]):
            writer.writerow([table_name, column_name, schema[table_name][column_name]])
    return output.getvalue()


def format_schema(schema: SchemaDict, output_format: str = "text") -> str:
    """Format resolved schema in the specified format.

    Args:
        schema: Resolved schema dictionary.
        output_format: One of "text", "json", or "csv".

    Returns:
        Formatted string.

    Raises:
        ValueError: If output_format is not recognized.
    """
    formatters = {
        "text": format_schema_text,
        "json": format_schema_json,
        "csv": format_schema_csv,
    }
    formatter = formatters.get(output_format)
    if formatter is None:
        raise ValueError(
            f"Invalid schema format '{output_format}'. Use 'text', 'json', or 'csv'."
        )
    return formatter(schema)
