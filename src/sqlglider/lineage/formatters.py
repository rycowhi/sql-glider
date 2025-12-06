"""Output formatters for lineage results."""

import csv
import json
from io import StringIO
from pathlib import Path
from typing import List, Optional

from sqlglider.lineage.analyzer import (
    ColumnLineage,
    QueryLineage,
    QueryTableLineage,
    TableLineage,
)


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

    @staticmethod
    def format_multi_query(query_results: List[QueryLineage]) -> str:
        """
        Format multi-query lineage as text.

        Output format:
        ==========
        Query 0: SELECT ...
        ==========
        ----------
        output_column
        ----------
        source1
        source2

        ==========
        Query 1: INSERT ...
        ==========
        ----------
        output_column2
        ----------
        source3

        Args:
            query_results: List of QueryLineage objects

        Returns:
            Formatted text string
        """
        output = []

        for query_result in query_results:
            # Query header
            output.append("=" * 10)
            output.append(f"Query {query_result.query_index}: {query_result.query_preview}")
            output.append("=" * 10)

            # Column lineage for this query
            for col_result in query_result.column_lineage:
                output.append("-" * 10)
                output.append(col_result.output_column)
                output.append("-" * 10)
                for source in col_result.source_columns:
                    output.append(source)

            # Add blank line between queries
            output.append("")

        return "\n".join(output)

    @staticmethod
    def format_multi_query_table(query_results: List[QueryTableLineage]) -> str:
        """
        Format multi-query table lineage as text.

        Args:
            query_results: List of QueryTableLineage objects

        Returns:
            Formatted text string
        """
        output = []

        for query_result in query_results:
            # Query header
            output.append("=" * 10)
            output.append(f"Query {query_result.query_index}: {query_result.query_preview}")
            output.append("=" * 10)

            # Table lineage for this query
            output.append("-" * 10)
            output.append(query_result.table_lineage.output_table)
            output.append("-" * 10)
            for source in query_result.table_lineage.source_tables:
                output.append(source)

            # Add blank line between queries
            output.append("")

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

    @staticmethod
    def format_multi_query(query_results: List[QueryLineage]) -> str:
        """
        Format multi-query lineage as JSON.

        Output format:
        {
          "queries": [
            {
              "query_index": 0,
              "query_preview": "SELECT ...",
              "column_lineage": [...]
            }
          ]
        }

        Args:
            query_results: List of QueryLineage objects

        Returns:
            JSON formatted string
        """
        data = {
            "queries": [result.model_dump() for result in query_results]
        }
        return json.dumps(data, indent=2)

    @staticmethod
    def format_multi_query_table(query_results: List[QueryTableLineage]) -> str:
        """
        Format multi-query table lineage as JSON.

        Args:
            query_results: List of QueryTableLineage objects

        Returns:
            JSON formatted string
        """
        data = {
            "queries": [result.model_dump() for result in query_results]
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

    @staticmethod
    def format_multi_query(query_results: List[QueryLineage]) -> str:
        """
        Format multi-query lineage as CSV.

        Output format:
        query_index,query_preview,output_column,source_table,source_column
        0,"SELECT ...",columnA,table1,col1
        0,"SELECT ...",columnA,table1,col2
        1,"INSERT ...",columnB,table2,col3

        Args:
            query_results: List of QueryLineage objects

        Returns:
            CSV formatted string
        """
        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(["query_index", "query_preview", "output_column", "source_table", "source_column"])

        # Write rows
        for query_result in query_results:
            for col_result in query_result.column_lineage:
                for source in col_result.source_columns:
                    # Parse fully qualified name to extract table and column
                    parts = source.rsplit(".", 1)
                    if len(parts) == 2:
                        source_table, source_column = parts
                    else:
                        # If no table prefix, use empty string
                        source_table = ""
                        source_column = source

                    writer.writerow([
                        query_result.query_index,
                        query_result.query_preview,
                        col_result.output_column,
                        source_table,
                        source_column
                    ])

        return output.getvalue()

    @staticmethod
    def format_multi_query_table(query_results: List[QueryTableLineage]) -> str:
        """
        Format multi-query table lineage as CSV.

        Args:
            query_results: List of QueryTableLineage objects

        Returns:
            CSV formatted string
        """
        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(["query_index", "query_preview", "output_table", "source_table"])

        # Write rows
        for query_result in query_results:
            for source in query_result.table_lineage.source_tables:
                writer.writerow([
                    query_result.query_index,
                    query_result.query_preview,
                    query_result.table_lineage.output_table,
                    source
                ])

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
