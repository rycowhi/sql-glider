"""Output formatters for lineage results."""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from sqlglider.lineage.analyzer import ColumnLineage, TableLineage


class TextFormatter:
    """Format lineage results as plain text."""

    @staticmethod
    def format(lineage_results: List[ColumnLineage]) -> str:
        """
        Format column lineage as text.

        Output format:
        ----------
        output_column
        ----------
        source1
        source2
        ----------
        output_column2
        ----------
        source3

        Args:
            lineage_results: List of ColumnLineage objects

        Returns:
            Formatted text string
        """
        output = []

        for result in lineage_results:
            output.append("-" * 10)
            output.append(result.output_column)
            output.append("-" * 10)
            for source in result.source_columns:
                output.append(source)

        return "\n".join(output)

    @staticmethod
    def format_table(lineage_result: TableLineage) -> str:
        """
        Format table lineage as text.

        Args:
            lineage_result: TableLineage object

        Returns:
            Formatted text string
        """
        output = []
        output.append("-" * 10)
        output.append(lineage_result.output_table)
        output.append("-" * 10)
        for source in lineage_result.source_tables:
            output.append(source)

        return "\n".join(output)


class JsonFormatter:
    """Format lineage results as JSON."""

    @staticmethod
    def format(lineage_results: List[ColumnLineage]) -> str:
        """
        Format column lineage as JSON.

        Output format:
        {
          "columns": [
            {
              "output_column": "columnA",
              "source_columns": ["table.col1", "table.col2"]
            }
          ]
        }

        Args:
            lineage_results: List of ColumnLineage objects

        Returns:
            JSON formatted string
        """
        data = {
            "columns": [result.model_dump() for result in lineage_results]
        }
        return json.dumps(data, indent=2)

    @staticmethod
    def format_table(lineage_result: TableLineage) -> str:
        """
        Format table lineage as JSON.

        Args:
            lineage_result: TableLineage object

        Returns:
            JSON formatted string
        """
        data = {
            "table": lineage_result.model_dump()
        }
        return json.dumps(data, indent=2)


class CsvFormatter:
    """Format lineage results as CSV."""

    @staticmethod
    def format(lineage_results: List[ColumnLineage]) -> str:
        """
        Format column lineage as CSV.

        Output format:
        output_column,source_table,source_column
        columnA,table1,col1
        columnA,table1,col2

        Args:
            lineage_results: List of ColumnLineage objects

        Returns:
            CSV formatted string
        """
        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(["output_column", "source_table", "source_column"])

        # Write rows
        for result in lineage_results:
            for source in result.source_columns:
                # Parse fully qualified name to extract table and column
                parts = source.rsplit(".", 1)
                if len(parts) == 2:
                    source_table, source_column = parts
                else:
                    # If no table prefix, use empty string
                    source_table = ""
                    source_column = source

                writer.writerow([result.output_column, source_table, source_column])

        return output.getvalue()

    @staticmethod
    def format_table(lineage_result: TableLineage) -> str:
        """
        Format table lineage as CSV.

        Args:
            lineage_result: TableLineage object

        Returns:
            CSV formatted string
        """
        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(["output_table", "source_table"])

        # Write rows
        for source in lineage_result.source_tables:
            writer.writerow([lineage_result.output_table, source])

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
