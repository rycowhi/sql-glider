"""Output formatters for lineage results."""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.text import Text

from sqlglider.lineage.analyzer import QueryLineageResult, QueryTablesResult


class TextFormatter:
    """Format lineage results as Rich tables for terminal display."""

    @staticmethod
    def format(results: List[QueryLineageResult], console: Console) -> None:
        """
        Format and print lineage results as Rich tables.

        Creates a styled table for each query showing output columns and their sources.
        For column-level lineage, shows Output Column and Source Column.
        For table-level lineage, shows Output Table and Source Table.

        Args:
            results: List of QueryLineageResult objects
            console: Rich Console instance for output
        """
        if not results:
            console.print("[yellow]No lineage results found.[/yellow]")
            return

        for i, result in enumerate(results):
            # Add spacing between tables (except for first)
            if i > 0:
                console.print()

            # Determine column headers based on level
            if result.level == "column":
                output_header = "Output Column"
                source_header = "Source Column"
            else:
                output_header = "Output Table"
                source_header = "Source Table"

            # Create table with query info as title
            title = (
                f"Query {result.metadata.query_index}: {result.metadata.query_preview}"
            )
            table = Table(title=title, title_style="bold")

            table.add_column(output_header, style="cyan")
            table.add_column(source_header, style="green")

            # Group lineage items by output_name
            output_groups: dict[str, list[str]] = {}
            for item in result.lineage_items:
                if item.output_name not in output_groups:
                    output_groups[item.output_name] = []
                if item.source_name:  # Skip empty sources
                    output_groups[item.output_name].append(item.source_name)

            # Add rows to table
            row_count = 0
            for output_name in sorted(output_groups.keys()):
                sources = sorted(output_groups[output_name])
                if sources:
                    # First source gets the output name
                    table.add_row(output_name, sources[0])
                    row_count += 1
                    # Additional sources get empty output column
                    for source in sources[1:]:
                        table.add_row("", source)
                        row_count += 1
                else:
                    # No sources found for this output
                    table.add_row(output_name, Text("(no sources)", style="dim"))
                    row_count += 1

            console.print(table)
            console.print(f"[dim]Total: {row_count} row(s)[/dim]")


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


class TableTextFormatter:
    """Format table analysis results as Rich tables for terminal display."""

    @staticmethod
    def format(results: List[QueryTablesResult], console: Console) -> None:
        """
        Format and print table analysis results as Rich tables.

        Creates a styled table for each query showing table names, usage, and types.

        Args:
            results: List of QueryTablesResult objects
            console: Rich Console instance for output
        """
        if not results:
            console.print("[yellow]No tables found.[/yellow]")
            return

        for i, result in enumerate(results):
            # Add spacing between tables (except for first)
            if i > 0:
                console.print()

            # Create table with query info as title
            title = (
                f"Query {result.metadata.query_index}: {result.metadata.query_preview}"
            )
            table = Table(title=title, title_style="bold")

            table.add_column("Table Name", style="cyan")
            table.add_column("Usage", style="green")
            table.add_column("Type", style="yellow")

            # Add rows
            for table_info in result.tables:
                table.add_row(
                    table_info.name,
                    table_info.usage.value,
                    table_info.object_type.value,
                )

            console.print(table)
            console.print(f"[dim]Total: {len(result.tables)} table(s)[/dim]")


class TableJsonFormatter:
    """Format table analysis results as JSON."""

    @staticmethod
    def format(results: List[QueryTablesResult]) -> str:
        """
        Format table analysis results as JSON.

        Output format:
        {
          "queries": [
            {
              "query_index": 0,
              "query_preview": "SELECT ...",
              "tables": [
                {"name": "schema.table", "usage": "INPUT", "object_type": "UNKNOWN"}
              ]
            }
          ]
        }

        Args:
            results: List of QueryTablesResult objects

        Returns:
            JSON-formatted string
        """
        queries = []
        for result in results:
            query_data = {
                "query_index": result.metadata.query_index,
                "query_preview": result.metadata.query_preview,
                "tables": [
                    {
                        "name": table_info.name,
                        "usage": table_info.usage.value,
                        "object_type": table_info.object_type.value,
                    }
                    for table_info in result.tables
                ],
            }
            queries.append(query_data)

        return json.dumps({"queries": queries}, indent=2)


class TableCsvFormatter:
    """Format table analysis results as CSV."""

    @staticmethod
    def format(results: List[QueryTablesResult]) -> str:
        """
        Format table analysis results as CSV.

        Output format:
        query_index,table_name,usage,object_type
        0,schema.table,INPUT,UNKNOWN
        0,schema.other_table,OUTPUT,TABLE

        Args:
            results: List of QueryTablesResult objects

        Returns:
            CSV-formatted string
        """
        if not results:
            return ""

        output = StringIO()
        headers = ["query_index", "table_name", "usage", "object_type"]

        writer = csv.writer(output)
        writer.writerow(headers)

        # Write data rows
        for result in results:
            query_index = result.metadata.query_index
            for table_info in result.tables:
                writer.writerow(
                    [
                        query_index,
                        table_info.name,
                        table_info.usage.value,
                        table_info.object_type.value,
                    ]
                )

        return output.getvalue()
