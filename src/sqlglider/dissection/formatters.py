"""Output formatters for dissection results."""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table

from sqlglider.dissection.models import QueryDissectionResult


class DissectionTextFormatter:
    """Format dissection results as Rich tables for terminal display."""

    @staticmethod
    def format(results: List[QueryDissectionResult], console: Console) -> None:
        """
        Format and print dissection results as Rich tables.

        Creates a styled table for each query showing all extracted components.

        Args:
            results: List of QueryDissectionResult objects
            console: Rich Console instance for output
        """
        if not results:
            console.print("[yellow]No dissection results found.[/yellow]")
            return

        for i, result in enumerate(results):
            # Add spacing between queries (except for first)
            if i > 0:
                console.print()

            # Create table with query info as title
            title = (
                f"Query {result.metadata.query_index} "
                f"({result.metadata.statement_type}): "
                f"{result.metadata.query_preview}"
            )
            table = Table(title=title, title_style="bold")

            table.add_column("Index", style="dim", width=6)
            table.add_column("Type", style="cyan", width=16)
            table.add_column("Name", style="green", min_width=10)
            table.add_column("Depth", style="yellow", width=6)
            table.add_column("Exec?", style="magenta", width=6)
            table.add_column("Location", style="blue", min_width=15)
            table.add_column("SQL Preview", style="dim", min_width=30)

            # Add rows for each component
            for component in result.components:
                # Truncate SQL for preview
                sql_preview = " ".join(component.sql.split())[:50]
                if len(component.sql) > 50:
                    sql_preview += "..."

                table.add_row(
                    str(component.component_index),
                    component.component_type.value,
                    component.name or "-",
                    str(component.depth),
                    "Yes" if component.is_executable else "No",
                    component.location[:35] + "..."
                    if len(component.location) > 35
                    else component.location,
                    sql_preview,
                )

            console.print(table)
            console.print(
                f"[dim]Total components: {result.metadata.total_components}[/dim]"
            )


class DissectionJsonFormatter:
    """Format dissection results as JSON."""

    @staticmethod
    def format(results: List[QueryDissectionResult]) -> str:
        """
        Format dissection results as JSON.

        Output format:
        {
          "queries": [
            {
              "query_index": 0,
              "query_preview": "SELECT ...",
              "statement_type": "INSERT",
              "total_components": 5,
              "components": [
                {
                  "component_type": "CTE",
                  "component_index": 0,
                  "name": "order_totals",
                  "sql": "SELECT ...",
                  "parent_index": null,
                  "depth": 0,
                  "is_executable": true,
                  "dependencies": [],
                  "location": "WITH clause"
                }
              ],
              "original_sql": "WITH order_totals AS ..."
            }
          ]
        }

        Args:
            results: List of QueryDissectionResult objects

        Returns:
            JSON-formatted string
        """
        queries = []
        for result in results:
            query_data = {
                "query_index": result.metadata.query_index,
                "query_preview": result.metadata.query_preview,
                "statement_type": result.metadata.statement_type,
                "total_components": result.metadata.total_components,
                "components": [
                    {
                        "component_type": component.component_type.value,
                        "component_index": component.component_index,
                        "name": component.name,
                        "sql": component.sql,
                        "parent_index": component.parent_index,
                        "depth": component.depth,
                        "is_executable": component.is_executable,
                        "dependencies": component.dependencies,
                        "location": component.location,
                    }
                    for component in result.components
                ],
                "original_sql": result.original_sql,
            }
            queries.append(query_data)

        return json.dumps({"queries": queries}, indent=2)


class DissectionCsvFormatter:
    """Format dissection results as CSV."""

    @staticmethod
    def format(results: List[QueryDissectionResult]) -> str:
        """
        Format dissection results as CSV.

        Output format:
        query_index,component_index,component_type,name,depth,is_executable,location,dependencies,sql
        0,0,CTE,order_totals,0,true,WITH clause,,"SELECT ..."

        Args:
            results: List of QueryDissectionResult objects

        Returns:
            CSV-formatted string
        """
        if not results:
            return ""

        output = StringIO()
        headers = [
            "query_index",
            "component_index",
            "component_type",
            "name",
            "depth",
            "is_executable",
            "location",
            "dependencies",
            "sql",
        ]

        writer = csv.writer(output)
        writer.writerow(headers)

        # Write data rows
        for result in results:
            query_index = result.metadata.query_index
            for component in result.components:
                # Join dependencies with semicolon
                deps_str = ";".join(component.dependencies)
                writer.writerow(
                    [
                        query_index,
                        component.component_index,
                        component.component_type.value,
                        component.name or "",
                        component.depth,
                        "true" if component.is_executable else "false",
                        component.location,
                        deps_str,
                        component.sql,
                    ]
                )

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
