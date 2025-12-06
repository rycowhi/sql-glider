"""CLI entry point for SQL Glider."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from sqlglot.errors import ParseError

from sqlglider.lineage.analyzer import LineageAnalyzer
from sqlglider.lineage.formatters import (
    CsvFormatter,
    JsonFormatter,
    OutputWriter,
    TextFormatter,
)
from sqlglider.utils.config import load_config
from sqlglider.utils.file_utils import read_sql_file

app = typer.Typer(
    name="sqlglider",
    help="SQL Utility Toolkit for better understanding, use, and governance of your queries.",
    invoke_without_command=False,
)
console = Console()
err_console = Console(stderr=True)


@app.callback()
def main():
    """SQL Glider - SQL Utility Toolkit."""
    pass


@app.command()
def lineage(
    sql_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to SQL file to analyze",
    ),
    level: Optional[str] = typer.Option(
        None,
        "--level",
        "-l",
        help="Analysis level: 'column' or 'table' (default: column, or from config)",
    ),
    dialect: Optional[str] = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (default: spark, or from config)",
    ),
    column: Optional[str] = typer.Option(
        None,
        "--column",
        "-c",
        help="Specific output column for forward lineage (default: all columns)",
    ),
    source_column: Optional[str] = typer.Option(
        None,
        "--source-column",
        "-s",
        help="Source column for reverse lineage (impact analysis)",
    ),
    table_filter: Optional[str] = typer.Option(
        None,
        "--table",
        "-t",
        help="Filter to only queries that reference this table (for multi-query files)",
    ),
    output_format: Optional[str] = typer.Option(
        None,
        "--output-format",
        "-f",
        help="Output format: 'text', 'json', or 'csv' (default: text, or from config)",
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output-file",
        "-o",
        help="Write output to file instead of stdout",
    ),
) -> None:
    """
    Analyze column or table lineage for a SQL file.

    Configuration can be set in sqlglider.toml in the current directory.
    CLI arguments override configuration file values.

    Examples:

        # Forward lineage: Find sources for output column
        sqlglider lineage query.sql --column order_total

        # Reverse lineage: Find outputs affected by source column
        sqlglider lineage query.sql --source-column orders.customer_id

        # Analyze all columns (forward lineage)
        sqlglider lineage query.sql

        # Analyze table-level lineage
        sqlglider lineage query.sql --level table

        # Export to JSON
        sqlglider lineage query.sql --output-format json --output-file lineage.json

        # Use different SQL dialect
        sqlglider lineage query.sql --dialect postgres
    """
    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    dialect = dialect or config.dialect or "spark"
    level = level or config.level or "column"
    output_format = output_format or config.output_format or "text"
    # Validate level
    if level not in ["column", "table"]:
        err_console.print(
            f"[red]Error:[/red] Invalid level '{level}'. Use 'column' or 'table'."
        )
        raise typer.Exit(1)

    # Validate output format
    if output_format not in ["text", "json", "csv"]:
        err_console.print(
            f"[red]Error:[/red] Invalid output format '{output_format}'. "
            "Use 'text', 'json', or 'csv'."
        )
        raise typer.Exit(1)

    # Validate mutual exclusivity of column and source_column
    if column and source_column:
        err_console.print(
            "[red]Error:[/red] Cannot specify both --column and --source-column. "
            "Use --column for forward lineage or --source-column for reverse lineage."
        )
        raise typer.Exit(1)

    try:
        # Read SQL file
        sql = read_sql_file(sql_file)

        # Create analyzer
        analyzer = LineageAnalyzer(sql, dialect=dialect)

        # Check if we have multiple queries
        has_multiple_queries = len(analyzer.expressions) > 1

        # Analyze based on level
        if level == "column":
            # Column-level lineage
            if source_column:
                # Reverse lineage (impact analysis)
                if has_multiple_queries:
                    # Multi-query reverse lineage
                    query_results = analyzer.analyze_all_queries_reverse(source_column, table_filter)
                    # Format as multi-query results
                    if output_format == "text":
                        formatted = TextFormatter.format_multi_query(query_results)
                    elif output_format == "json":
                        formatted = JsonFormatter.format_multi_query(query_results)
                    else:  # csv
                        formatted = CsvFormatter.format_multi_query(query_results)
                else:
                    # Single-query reverse lineage
                    results = analyzer.analyze_reverse_lineage(source_column)
                    # Format as single-query results
                    if output_format == "text":
                        formatted = TextFormatter.format(results)
                    elif output_format == "json":
                        formatted = JsonFormatter.format(results)
                    else:  # csv
                        formatted = CsvFormatter.format(results)
            else:
                # Forward lineage (source tracing)
                if has_multiple_queries:
                    # Multi-query mode
                    query_results = analyzer.analyze_all_queries(column, table_filter)
                    # Format as multi-query results
                    if output_format == "text":
                        formatted = TextFormatter.format_multi_query(query_results)
                    elif output_format == "json":
                        formatted = JsonFormatter.format_multi_query(query_results)
                    else:  # csv
                        formatted = CsvFormatter.format_multi_query(query_results)
                else:
                    # Single-query mode (backward compatibility)
                    results = analyzer.analyze_column_lineage(column)
                    # Format as single-query results
                    if output_format == "text":
                        formatted = TextFormatter.format(results)
                    elif output_format == "json":
                        formatted = JsonFormatter.format(results)
                    else:  # csv
                        formatted = CsvFormatter.format(results)

        else:  # table
            # Table-level lineage
            if has_multiple_queries:
                # Multi-query table lineage
                query_results = analyzer.analyze_all_queries_table_lineage(table_filter)
                # Format as multi-query table results
                if output_format == "text":
                    formatted = TextFormatter.format_multi_query_table(query_results)
                elif output_format == "json":
                    formatted = JsonFormatter.format_multi_query_table(query_results)
                else:  # csv
                    formatted = CsvFormatter.format_multi_query_table(query_results)
            else:
                # Single-query table lineage
                result = analyzer.analyze_table_lineage()

                # Format output
                if output_format == "text":
                    formatted = TextFormatter.format_table(result)
                elif output_format == "json":
                    formatted = JsonFormatter.format_table(result)
                else:  # csv
                    formatted = CsvFormatter.format_table(result)

        # Write output
        OutputWriter.write(formatted, output_file)

        if output_file:
            console.print(f"[green]Success:[/green] Lineage written to {output_file}")

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
