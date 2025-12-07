"""Output formatters for lineage results."""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from sqlglider.lineage.analyzer import QueryLineageResult


class TextFormatter:
    """Format lineage results as plain text."""

    @staticmethod
    def format(results: List[QueryLineageResult]) -> str:
        """
        Format lineage results as text.

        Output format for column lineage:
        ==========
        Query 0: SELECT ...
        ==========
        ----------
        output_column
        ----------
        source1
        source2

        Output format for table lineage:
        ==========
        Query 0: SELECT ...
        ==========
        ----------
        output_table
        ----------
        source_table1
        source_table2

        Args:
            results: List of QueryLineageResult objects

        Returns:
            Formatted text string
        """
        if not results:
            return ""

        output = []

        for result in results:
            # Query header
            output.append("=" * 10)
            output.append(
                f"Query {result.metadata.query_index}: {result.metadata.query_preview}"
            )
            output.append("=" * 10)

            # Group lineage items by output_name
            output_groups: dict[str, list[str]] = {}
            for item in result.lineage_items:
                if item.output_name not in output_groups:
                    output_groups[item.output_name] = []
                if item.source_name:  # Skip empty sources
                    output_groups[item.output_name].append(item.source_name)

            # Format each output group
            for output_name in sorted(output_groups.keys()):
                output.append("-" * 10)
                output.append(output_name)
                output.append("-" * 10)
                for source in sorted(output_groups[output_name]):
                    output.append(source)

        return "\n".join(output)


class JsonFormatter:
    """Format lineage results as JSON."""

    @staticmethod
    def format(results: List[QueryLineageResult]) -> str:
        """
        Format lineage results as JSON.

        Output format:
        {
          "queries": [
            {
              "query_index": 0,
              "query_preview": "SELECT ...",
              "level": "column",
              "lineage": [
                {"output_name": "table.col_a", "source_name": "src.col_x"},
                {"output_name": "table.col_a", "source_name": "src.col_y"}
              ]
            }
          ]
        }

        Args:
            results: List of QueryLineageResult objects

        Returns:
            JSON-formatted string
        """
        queries = []
        for result in results:
            query_data = {
                "query_index": result.metadata.query_index,
                "query_preview": result.metadata.query_preview,
                "level": result.level,
                "lineage": [
                    {
                        "output_name": item.output_name,
                        "source_name": item.source_name,
                    }
                    for item in result.lineage_items
                ],
            }
            queries.append(query_data)

        return json.dumps({"queries": queries}, indent=2)


class CsvFormatter:
    """Format lineage results as CSV."""

    @staticmethod
    def format(results: List[QueryLineageResult]) -> str:
        """
        Format lineage results as CSV.

        Column-level output format:
        query_index,output_column,source_column
        0,table.column_a,source_table.column_x
        0,table.column_a,source_table.column_y
        0,table.column_b,source_table2.column_z

        Table-level output format:
        query_index,output_table,source_table
        0,query_result,customers
        0,query_result,orders

        Args:
            results: List of QueryLineageResult objects

        Returns:
            CSV-formatted string
        """
        if not results:
            return ""

        output = StringIO()

        # Determine column headers based on level
        level = results[0].level if results else "column"

        if level == "column":
            headers = ["query_index", "output_column", "source_column"]
        else:  # table
            headers = ["query_index", "output_table", "source_table"]

        writer = csv.writer(output)
        writer.writerow(headers)

        # Write data rows
        for result in results:
            query_index = result.metadata.query_index
            for item in result.lineage_items:
                writer.writerow([query_index, item.output_name, item.source_name])

        return output.getvalue()


class OutputWriter:
    """Write formatted output to file or stdout."""

    @staticmethod
    def write(content: str, output_file: Optional[Path] = None) -> None:
        """
        Write content to file or stdout.

        Args:
            content: The content to write
            output_file: Optional file path. If None, writes to stdout.
        """
        if output_file:
            output_file.write_text(content, encoding="utf-8")
        else:
            print(content)
