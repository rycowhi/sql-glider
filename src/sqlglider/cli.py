"""CLI entry point for SQL Glider."""

import json
import sys
from pathlib import Path
from typing import Callable, List, Optional

import typer
from rich.console import Console
from rich.table import Table
from sqlglot.errors import ParseError
from typing_extensions import Annotated

from sqlglider.global_models import AnalysisLevel, NodeFormat
from sqlglider.lineage.analyzer import LineageAnalyzer
from sqlglider.lineage.formatters import (
    CsvFormatter,
    JsonFormatter,
    OutputWriter,
    TableCsvFormatter,
    TableJsonFormatter,
    TableTextFormatter,
    TextFormatter,
)
from sqlglider.templating import (
    TemplaterError,
    get_templater,
    list_templaters,
    load_all_variables,
)
from sqlglider.utils.config import load_config

app = typer.Typer(
    name="sqlglider",
    help="SQL Utility Toolkit for better understanding, use, and governance of your queries.",
    invoke_without_command=False,
)
console = Console()
err_console = Console(stderr=True)


def _apply_templating(
    sql: str,
    templater_name: Optional[str],
    cli_vars: Optional[List[str]],
    vars_file: Optional[Path],
    config,
    source_path: Optional[Path] = None,
) -> str:
    """Apply templating to SQL if a templater is specified.

    Args:
        sql: The SQL string to template.
        templater_name: Name of the templater to use (e.g., "jinja").
                       If None, returns sql unchanged.
        cli_vars: List of CLI variable strings in "key=value" format.
        vars_file: Path to a variables file (JSON or YAML).
        config: The loaded ConfigSettings object.
        source_path: Path to source file for resolving includes.

    Returns:
        The templated SQL string, or the original if no templater specified.
    """
    if not templater_name:
        return sql

    # Get variables from config
    config_vars_file = None
    config_vars = None
    if config.templating:
        if config.templating.variables_file and not vars_file:
            config_vars_file = Path(config.templating.variables_file)
            if not config_vars_file.exists():
                err_console.print(
                    f"[yellow]Warning:[/yellow] Variables file from config "
                    f"not found: {config_vars_file}"
                )
                config_vars_file = None
        config_vars = config.templating.variables

    # Load variables from all sources
    variables = load_all_variables(
        cli_vars=cli_vars,
        vars_file=vars_file or config_vars_file,
        config_vars=config_vars,
        use_env=True,
    )

    # Get templater instance and render
    templater_instance = get_templater(templater_name)
    return templater_instance.render(sql, variables=variables, source_path=source_path)


@app.callback()
def main():
    """SQL Glider - SQL Utility Toolkit."""
    pass


@app.command()
def lineage(
    sql_file: Annotated[
        typer.FileText,
        typer.Argument(
            default_factory=lambda: sys.stdin,
            show_default="stdin",
            help="Path to SQL file to analyze (reads from stdin if not provided)",
        ),
    ],
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
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater for SQL preprocessing (e.g., 'jinja', 'none')",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
    no_star: bool = typer.Option(
        False,
        "--no-star",
        help="Fail if SELECT * cannot be resolved to actual columns",
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

        # Analyze templated SQL with Jinja2
        sqlglider lineage query.sql --templater jinja --var schema=analytics

        # Use variables file for templating
        sqlglider lineage query.sql --templater jinja --vars-file vars.json
    """
    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    dialect = dialect or config.dialect or "spark"
    level_str = level or config.level or "column"
    output_format = output_format or config.output_format or "text"
    templater = templater or config.templater  # None means no templating
    no_star = no_star or config.no_star or False
    # Validate and convert level to enum
    try:
        analysis_level = AnalysisLevel(level_str)
    except ValueError:
        err_console.print(
            f"[red]Error:[/red] Invalid level '{level_str}'. Use 'column' or 'table'."
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

    # Check if reading from stdin (cross-platform: name is "<stdin>" on all OS)
    is_stdin = sql_file.name == "<stdin>"

    try:
        # Check if stdin is being used without input
        if is_stdin and sys.stdin.isatty():
            err_console.print(
                "[red]Error:[/red] No SQL file provided and stdin is interactive. "
                "Provide a SQL file path or pipe SQL via stdin."
            )
            raise typer.Exit(1)

        # Read SQL from file or stdin
        sql = sql_file.read()

        # Determine source path for templating (None if stdin)
        source_path = None if is_stdin else Path(sql_file.name)

        # Apply templating if specified
        sql = _apply_templating(
            sql,
            templater_name=templater,
            cli_vars=var,
            vars_file=vars_file,
            config=config,
            source_path=source_path,
        )

        # Create analyzer
        analyzer = LineageAnalyzer(sql, dialect=dialect, no_star=no_star)

        # Unified lineage analysis (handles both single and multi-query files)
        results = analyzer.analyze_queries(
            level=analysis_level,
            column=column,
            source_column=source_column,
            table_filter=table_filter,
        )

        # Print warnings for skipped queries
        for skipped in analyzer.skipped_queries:
            err_console.print(
                f"[yellow]Warning:[/yellow] Skipping query {skipped.query_index} "
                f"({skipped.statement_type}): {skipped.reason}"
            )

        # Format and output based on output format
        if output_format == "text":
            if output_file:
                # For file output, use a string-based console to capture output
                from io import StringIO

                from rich.console import Console as FileConsole

                string_buffer = StringIO()
                file_console = FileConsole(file=string_buffer, force_terminal=False)
                TextFormatter.format(results, file_console)
                output_file.write_text(string_buffer.getvalue(), encoding="utf-8")
                console.print(
                    f"[green]Success:[/green] Lineage written to {output_file}"
                )
            else:
                # Direct console output with Rich formatting
                TextFormatter.format(results, console)
        elif output_format == "json":
            formatted = JsonFormatter.format(results)
            OutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Lineage written to {output_file}"
                )
        else:  # csv
            formatted = CsvFormatter.format(results)
            OutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Lineage written to {output_file}"
                )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


# Tables command group
tables_app = typer.Typer(
    name="tables",
    help="Table-related analysis commands.",
)
app.add_typer(tables_app, name="tables")


@tables_app.command("overview")
def tables_overview(
    sql_file: Annotated[
        typer.FileText,
        typer.Argument(
            default_factory=lambda: sys.stdin,
            show_default="stdin",
            help="Path to SQL file to analyze (reads from stdin if not provided)",
        ),
    ],
    dialect: Optional[str] = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (default: spark, or from config)",
    ),
    table_filter: Optional[str] = typer.Option(
        None,
        "--table",
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
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater for SQL preprocessing (e.g., 'jinja', 'none')",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
) -> None:
    """
    List all tables involved in a SQL file.

    Outputs table names with their usage type (INPUT, OUTPUT, or BOTH) and
    object type (TABLE, VIEW, CTE, or UNKNOWN).

    Configuration can be set in sqlglider.toml in the current directory.
    CLI arguments override configuration file values.

    Examples:

        # List all tables in a SQL file
        sqlglider tables overview query.sql

        # Export to JSON
        sqlglider tables overview query.sql --output-format json

        # Export to CSV file
        sqlglider tables overview query.sql --output-format csv --output-file tables.csv

        # Use different SQL dialect
        sqlglider tables overview query.sql --dialect postgres

        # Filter to queries referencing a specific table
        sqlglider tables overview query.sql --table customers

        # Analyze templated SQL with Jinja2
        sqlglider tables overview query.sql --templater jinja --var schema=analytics
    """
    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    dialect = dialect or config.dialect or "spark"
    output_format = output_format or config.output_format or "text"
    templater = templater or config.templater  # None means no templating

    # Validate output format
    if output_format not in ["text", "json", "csv"]:
        err_console.print(
            f"[red]Error:[/red] Invalid output format '{output_format}'. "
            "Use 'text', 'json', or 'csv'."
        )
        raise typer.Exit(1)

    # Check if reading from stdin (cross-platform: name is "<stdin>" on all OS)
    is_stdin = sql_file.name == "<stdin>"

    try:
        # Check if stdin is being used without input
        if is_stdin and sys.stdin.isatty():
            err_console.print(
                "[red]Error:[/red] No SQL file provided and stdin is interactive. "
                "Provide a SQL file path or pipe SQL via stdin."
            )
            raise typer.Exit(1)

        # Read SQL from file or stdin
        sql = sql_file.read()

        # Determine source path for templating (None if stdin)
        source_path = None if is_stdin else Path(sql_file.name)

        # Apply templating if specified
        sql = _apply_templating(
            sql,
            templater_name=templater,
            cli_vars=var,
            vars_file=vars_file,
            config=config,
            source_path=source_path,
        )

        # Create analyzer
        analyzer = LineageAnalyzer(sql, dialect=dialect)

        # Analyze tables
        results = analyzer.analyze_tables(table_filter=table_filter)

        # Format and output based on output format
        if output_format == "text":
            if output_file:
                # For file output, use a string-based console to capture output
                from io import StringIO

                from rich.console import Console as FileConsole

                string_buffer = StringIO()
                file_console = FileConsole(file=string_buffer, force_terminal=False)
                TableTextFormatter.format(results, file_console)
                output_file.write_text(string_buffer.getvalue(), encoding="utf-8")
                console.print(
                    f"[green]Success:[/green] Tables written to {output_file}"
                )
            else:
                # Direct console output with Rich formatting
                TableTextFormatter.format(results, console)
        elif output_format == "json":
            formatted = TableJsonFormatter.format(results)
            OutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Tables written to {output_file}"
                )
        else:  # csv
            formatted = TableCsvFormatter.format(results)
            OutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Tables written to {output_file}"
                )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


@tables_app.command("pull")
def tables_pull(
    sql_file: Annotated[
        typer.FileText,
        typer.Argument(
            default_factory=lambda: sys.stdin,
            show_default="stdin",
            help="Path to SQL file to analyze (reads from stdin if not provided)",
        ),
    ],
    catalog_type: Optional[str] = typer.Option(
        None,
        "--catalog-type",
        "-c",
        help="Catalog provider (e.g., 'databricks'). Required if not in config.",
    ),
    ddl_folder: Optional[Path] = typer.Option(
        None,
        "--ddl-folder",
        "-o",
        help="Output folder for DDL files. If not provided, outputs to stdout.",
    ),
    dialect: Optional[str] = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (default: spark, or from config)",
    ),
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater for SQL preprocessing (e.g., 'jinja', 'none')",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
    list_available: bool = typer.Option(
        False,
        "--list",
        "-l",
        help="List available catalog providers and exit",
    ),
) -> None:
    """
    Pull DDL definitions from a remote catalog for tables used in SQL.

    Analyzes the SQL file to find referenced tables, then fetches their DDL
    from the specified catalog provider (e.g., Databricks Unity Catalog).

    CTEs are automatically excluded since they don't exist in remote catalogs.

    Configuration can be set in sqlglider.toml in the current directory.
    CLI arguments override configuration file values.

    Examples:

        # Pull DDL for tables in a SQL file (output to stdout)
        sqlglider tables pull query.sql --catalog-type databricks

        # Pull DDL to a folder (one file per table)
        sqlglider tables pull query.sql -c databricks -o ./ddl/

        # Use config file for catalog settings
        sqlglider tables pull query.sql

        # With templating
        sqlglider tables pull query.sql -c databricks --templater jinja --var schema=prod

        # List available catalog providers
        sqlglider tables pull --list
    """
    from sqlglider.catalog import CatalogError, get_catalog, list_catalogs
    from sqlglider.lineage.analyzer import ObjectType

    # Handle --list option
    if list_available:
        available = list_catalogs()
        if available:
            console.print("[bold]Available catalog providers:[/bold]")
            for name in available:
                console.print(f"  - {name}")
        else:
            console.print(
                "[yellow]No catalog providers available.[/yellow]\n"
                "Install a provider with: pip install sql-glider[databricks]"
            )
        raise typer.Exit(0)

    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    dialect = dialect or config.dialect or "spark"
    templater = templater or config.templater  # None means no templating
    catalog_type = catalog_type or config.catalog_type
    ddl_folder_str = config.ddl_folder if ddl_folder is None else None
    if ddl_folder is None and ddl_folder_str:
        ddl_folder = Path(ddl_folder_str)

    # Validate catalog_type is provided
    if not catalog_type:
        err_console.print(
            "[red]Error:[/red] No catalog provider specified. "
            "Use --catalog-type or set catalog_type in sqlglider.toml."
        )
        raise typer.Exit(1)

    # Check if reading from stdin (cross-platform: name is "<stdin>" on all OS)
    is_stdin = sql_file.name == "<stdin>"

    try:
        # Check if stdin is being used without input
        if is_stdin and sys.stdin.isatty():
            err_console.print(
                "[red]Error:[/red] No SQL file provided and stdin is interactive. "
                "Provide a SQL file path or pipe SQL via stdin."
            )
            raise typer.Exit(1)

        # Read SQL from file or stdin
        sql = sql_file.read()

        # Determine source path for templating (None if stdin)
        source_path = None if is_stdin else Path(sql_file.name)

        # Apply templating if specified
        sql = _apply_templating(
            sql,
            templater_name=templater,
            cli_vars=var,
            vars_file=vars_file,
            config=config,
            source_path=source_path,
        )

        # Create analyzer and extract tables
        analyzer = LineageAnalyzer(sql, dialect=dialect)
        table_results = analyzer.analyze_tables()

        # Collect unique table names, excluding CTEs
        table_names: set[str] = set()
        for result in table_results:
            for table_info in result.tables:
                if table_info.object_type != ObjectType.CTE:
                    table_names.add(table_info.name)

        if not table_names:
            console.print("[yellow]No tables found in SQL (CTEs excluded).[/yellow]")
            raise typer.Exit(0)

        # Get catalog instance and configure it
        catalog = get_catalog(catalog_type)

        # Build catalog config from config file
        catalog_config: dict[str, str] = {}
        if (
            config.catalog
            and catalog_type == "databricks"
            and config.catalog.databricks
        ):
            db_config = config.catalog.databricks
            if db_config.warehouse_id:
                catalog_config["warehouse_id"] = db_config.warehouse_id
            if db_config.profile:
                catalog_config["profile"] = db_config.profile
            if db_config.host:
                catalog_config["host"] = db_config.host
            if db_config.token:
                catalog_config["token"] = db_config.token

        catalog.configure(catalog_config)

        # Fetch DDL for all tables
        console.print(
            f"[dim]Fetching DDL for {len(table_names)} table(s) from {catalog_type}...[/dim]"
        )
        ddl_results = catalog.get_ddl_batch(list(table_names))

        # Count successes and failures
        successes = 0
        failures = 0

        # Output DDL
        if ddl_folder:
            # Create output folder if it doesn't exist
            ddl_folder.mkdir(parents=True, exist_ok=True)

            for table_name, ddl in ddl_results.items():
                if ddl.startswith("ERROR:"):
                    err_console.print(f"[yellow]Warning:[/yellow] {table_name}: {ddl}")
                    failures += 1
                else:
                    # Write DDL to file named by table identifier
                    file_name = f"{table_name}.sql"
                    file_path = ddl_folder / file_name
                    file_path.write_text(ddl, encoding="utf-8")
                    successes += 1

            console.print(
                f"[green]Success:[/green] Wrote {successes} DDL file(s) to {ddl_folder}"
            )
            if failures > 0:
                console.print(
                    f"[yellow]Warning:[/yellow] {failures} table(s) failed to fetch"
                )
        else:
            # Output to stdout
            for table_name, ddl in ddl_results.items():
                if ddl.startswith("ERROR:"):
                    err_console.print(f"[yellow]Warning:[/yellow] {table_name}: {ddl}")
                    failures += 1
                else:
                    print(f"-- Table: {table_name}")
                    print(ddl)
                    print()
                    successes += 1

            if failures > 0:
                err_console.print(
                    f"\n[yellow]Warning:[/yellow] {failures} table(s) failed to fetch"
                )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except CatalogError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


@app.command()
def template(
    sql_file: Annotated[
        typer.FileText,
        typer.Argument(
            default_factory=lambda: sys.stdin,
            show_default="stdin",
            help="Path to SQL template file to render (reads from stdin if not provided)",
        ),
    ],
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater to use (default: jinja, or from config)",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output-file",
        "-o",
        help="Write output to file instead of stdout",
    ),
    list_available: bool = typer.Option(
        False,
        "--list",
        "-l",
        help="List available templaters and exit",
    ),
) -> None:
    """
    Render a SQL template file with variable substitution.

    Uses the specified templater (default: jinja) to process the SQL file
    with template variables. Variables can be provided via CLI, file, or config.

    Configuration can be set in sqlglider.toml in the current directory.
    CLI arguments override configuration file values.

    Examples:

        # Basic template rendering
        sqlglider template query.sql --var schema=analytics --var table=users

        # Using a variables file
        sqlglider template query.sql --vars-file vars.json

        # Output to file
        sqlglider template query.sql --var schema=prod -o rendered.sql

        # List available templaters
        sqlglider template query.sql --list

        # Use specific templater
        sqlglider template query.sql --templater jinja --var name=test
    """
    # Handle --list option
    if list_available:
        available = list_templaters()
        if available:
            console.print("[bold]Available templaters:[/bold]")
            for name in available:
                console.print(f"  - {name}")
        else:
            console.print("[yellow]No templaters available[/yellow]")
        raise typer.Exit(0)

    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    # For template command, default to "jinja" (always apply templating)
    templater = templater or config.templater or "jinja"

    # Check if reading from stdin (cross-platform: name is "<stdin>" on all OS)
    is_stdin = sql_file.name == "<stdin>"

    try:
        # Check if stdin is being used without input
        if is_stdin and sys.stdin.isatty():
            err_console.print(
                "[red]Error:[/red] No SQL file provided and stdin is interactive. "
                "Provide a SQL file path or pipe SQL via stdin."
            )
            raise typer.Exit(1)

        # Read SQL from file or stdin
        sql = sql_file.read()

        # Determine source path for templating (None if stdin)
        source_path = None if is_stdin else Path(sql_file.name)

        # Apply templating (always for template command)
        rendered = _apply_templating(
            sql,
            templater_name=templater,
            cli_vars=var,
            vars_file=vars_file,
            config=config,
            source_path=source_path,
        )

        # Write output
        if output_file:
            output_file.write_text(rendered, encoding="utf-8")
            console.print(
                f"[green]Success:[/green] Rendered SQL written to {output_file}"
            )
        else:
            print(rendered)

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


# Graph command group
graph_app = typer.Typer(
    name="graph",
    help="Graph-based lineage analysis commands.",
)
app.add_typer(graph_app, name="graph")


@graph_app.command("build")
def graph_build(
    paths: List[Path] = typer.Argument(
        None,
        help="SQL file(s) or directory path to process",
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Output file path for serialized graph (required)",
    ),
    recursive: bool = typer.Option(
        False,
        "--recursive",
        "-r",
        help="Recursively search directories for SQL files",
    ),
    glob_pattern: str = typer.Option(
        "*.sql",
        "--glob",
        "-g",
        help="Glob pattern for matching SQL files in directories",
    ),
    manifest: Optional[Path] = typer.Option(
        None,
        "--manifest",
        "-m",
        exists=True,
        help="Path to manifest CSV file with file_path and optional dialect columns",
    ),
    node_format: str = typer.Option(
        "qualified",
        "--node-format",
        "-n",
        help="Node identifier format: 'qualified' or 'structured'",
    ),
    dialect: Optional[str] = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (default: spark, falls back if not in manifest)",
    ),
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater for SQL preprocessing (e.g., 'jinja', 'none')",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
    no_star: bool = typer.Option(
        False,
        "--no-star",
        help="Fail if SELECT * cannot be resolved to actual columns",
    ),
) -> None:
    """
    Build a lineage graph from SQL files.

    Supports multiple input modes:
    - Single file: sqlglider graph build query.sql -o graph.json
    - Multiple files: sqlglider graph build query1.sql query2.sql -o graph.json
    - Directory: sqlglider graph build ./queries/ -r -o graph.json
    - Manifest: sqlglider graph build --manifest manifest.csv -o graph.json

    Examples:

        # Build from single file
        sqlglider graph build query.sql -o graph.json

        # Build from directory (recursive)
        sqlglider graph build ./queries/ -r -o graph.json

        # Build from manifest with custom dialect
        sqlglider graph build --manifest manifest.csv -o graph.json --dialect postgres

        # Build with structured node format
        sqlglider graph build query.sql -o graph.json --node-format structured

        # Build with Jinja2 templating
        sqlglider graph build ./queries/ -o graph.json --templater jinja --var schema=prod
    """
    from sqlglider.graph.builder import GraphBuilder
    from sqlglider.graph.serialization import save_graph

    # Load config for defaults
    config = load_config()
    dialect = dialect or config.dialect or "spark"
    templater = templater or config.templater  # None means no templating
    no_star = no_star or config.no_star or False

    # Validate and convert node format to enum
    try:
        node_format_enum = NodeFormat(node_format)
    except ValueError:
        err_console.print(
            f"[red]Error:[/red] Invalid node format '{node_format}'. "
            "Use 'qualified' or 'structured'."
        )
        raise typer.Exit(1)

    # Validate inputs
    if not paths and not manifest:
        err_console.print(
            "[red]Error:[/red] Must provide either file/directory paths or --manifest option."
        )
        raise typer.Exit(1)

    # Create SQL preprocessor if templating is enabled
    sql_preprocessor: Optional[Callable[[str, Path], str]] = None
    if templater:
        # Load variables once for all files
        config_vars_file = None
        config_vars = None
        if config.templating:
            if config.templating.variables_file and not vars_file:
                config_vars_file = Path(config.templating.variables_file)
                if not config_vars_file.exists():
                    err_console.print(
                        f"[yellow]Warning:[/yellow] Variables file from config "
                        f"not found: {config_vars_file}"
                    )
                    config_vars_file = None
            config_vars = config.templating.variables

        variables = load_all_variables(
            cli_vars=var,
            vars_file=vars_file or config_vars_file,
            config_vars=config_vars,
            use_env=True,
        )

        templater_instance = get_templater(templater)

        def _preprocess(sql: str, file_path: Path) -> str:
            return templater_instance.render(
                sql, variables=variables, source_path=file_path
            )

        sql_preprocessor = _preprocess

    try:
        builder = GraphBuilder(
            node_format=node_format_enum,
            dialect=dialect,
            sql_preprocessor=sql_preprocessor,
            no_star=no_star,
        )

        # Process manifest if provided
        if manifest:
            builder.add_manifest(manifest, dialect=dialect)

        # Process paths - collect all files first for progress tracking
        if paths:
            all_files: list[Path] = []
            for path in paths:
                if path.is_dir():
                    pattern = f"**/{glob_pattern}" if recursive else glob_pattern
                    all_files.extend(
                        f for f in sorted(path.glob(pattern)) if f.is_file()
                    )
                elif path.is_file():
                    all_files.append(path)
                else:
                    err_console.print(f"[red]Error:[/red] Path not found: {path}")
                    raise typer.Exit(1)
            builder.add_files(all_files, dialect=dialect)

        # Build and save graph
        graph = builder.build()
        save_graph(graph, output)

        console.print(
            f"[green]Success:[/green] Graph saved to {output} "
            f"({graph.metadata.total_nodes} nodes, {graph.metadata.total_edges} edges)"
        )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


@graph_app.command("merge")
def graph_merge(
    inputs: List[Path] = typer.Argument(
        None,
        help="JSON graph files to merge",
    ),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Output file path for merged graph (required)",
    ),
    glob_pattern: Optional[str] = typer.Option(
        None,
        "--glob",
        "-g",
        help="Glob pattern for matching graph JSON files (e.g., 'graphs/*.json')",
    ),
) -> None:
    """
    Merge multiple lineage graphs into one.

    Nodes are deduplicated by identifier. Edges are deduplicated by source-target pair.

    Examples:

        # Merge specific files
        sqlglider graph merge graph1.json graph2.json -o merged.json

        # Merge with glob pattern
        sqlglider graph merge --glob "graphs/*.json" -o merged.json

        # Combine both
        sqlglider graph merge extra.json --glob "graphs/*.json" -o merged.json
    """
    from sqlglider.graph.merge import GraphMerger
    from sqlglider.graph.serialization import save_graph

    # Validate inputs
    if not inputs and not glob_pattern:
        err_console.print(
            "[red]Error:[/red] Must provide either graph files or --glob option."
        )
        raise typer.Exit(1)

    try:
        merger = GraphMerger()

        # Process glob pattern if provided
        if glob_pattern:
            glob_files = sorted(Path(".").glob(glob_pattern))
            if not glob_files:
                err_console.print(
                    f"[yellow]Warning:[/yellow] No files matched pattern: {glob_pattern}"
                )
            for graph_file in glob_files:
                if graph_file.is_file():
                    merger.add_file(graph_file)

        # Process explicit inputs
        if inputs:
            for graph_file in inputs:
                if not graph_file.exists():
                    err_console.print(f"[red]Error:[/red] File not found: {graph_file}")
                    raise typer.Exit(1)
                merger.add_file(graph_file)

        # Merge and save
        merged_graph = merger.merge()
        save_graph(merged_graph, output)

        console.print(
            f"[green]Success:[/green] Merged graph saved to {output} "
            f"({merged_graph.metadata.total_nodes} nodes, {merged_graph.metadata.total_edges} edges, "
            f"{len(merged_graph.metadata.source_files)} source files)"
        )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


@graph_app.command("query")
def graph_query(
    graph_file: Path = typer.Argument(
        ...,
        exists=True,
        help="Path to graph JSON file",
    ),
    upstream: Optional[str] = typer.Option(
        None,
        "--upstream",
        "-u",
        help="Find all source columns that contribute to this column",
    ),
    downstream: Optional[str] = typer.Option(
        None,
        "--downstream",
        "-d",
        help="Find all columns affected by this source column",
    ),
    output_format: str = typer.Option(
        "text",
        "--output-format",
        "-f",
        help="Output format: 'text', 'json', or 'csv'",
    ),
) -> None:
    """
    Query a lineage graph for upstream or downstream dependencies.

    Examples:

        # Find all source columns for a target
        sqlglider graph query graph.json --upstream orders.customer_id

        # Find all columns affected by a source
        sqlglider graph query graph.json --downstream customers.customer_id

        # JSON output
        sqlglider graph query graph.json --upstream orders.total -f json

        # CSV output
        sqlglider graph query graph.json --downstream orders.order_id -f csv
    """
    from sqlglider.graph.query import GraphQuerier

    # Validate options
    if not upstream and not downstream:
        err_console.print(
            "[red]Error:[/red] Must specify either --upstream or --downstream."
        )
        raise typer.Exit(1)

    if upstream and downstream:
        err_console.print(
            "[red]Error:[/red] Cannot specify both --upstream and --downstream. "
            "Choose one direction."
        )
        raise typer.Exit(1)

    if output_format not in ["text", "json", "csv"]:
        err_console.print(
            f"[red]Error:[/red] Invalid output format '{output_format}'. "
            "Use 'text', 'json', or 'csv'."
        )
        raise typer.Exit(1)

    try:
        querier = GraphQuerier.from_file(graph_file)

        if upstream:
            result = querier.find_upstream(upstream)
        else:
            assert downstream is not None  # Validated above
            result = querier.find_downstream(downstream)

        # Format and output
        if output_format == "text":
            _format_query_result_text(result)
        elif output_format == "json":
            _format_query_result_json(result)
        else:  # csv
            _format_query_result_csv(result)

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


def _format_query_result_text(result) -> None:
    """Format query result as text table."""
    direction_label = (
        "Sources" if result.direction == "upstream" else "Affected Columns"
    )

    table = Table(title=f"{direction_label} for '{result.query_column}'")
    table.add_column("Column", style="cyan")
    table.add_column("Table", style="green")
    table.add_column("Hops", style="yellow", justify="right")
    table.add_column("Root", style="magenta", justify="center")
    table.add_column("Leaf", style="magenta", justify="center")
    table.add_column("Paths", style="dim")
    table.add_column("File", style="dim")

    for node in result.related_columns:
        # Format paths as newline-separated arrow strings
        paths_str = (
            "\n".join(p.to_arrow_string() for p in node.paths) if node.paths else "-"
        )

        table.add_row(
            node.column or node.identifier,
            node.table or "",
            str(node.hops),
            "Y" if node.is_root else "N",
            "Y" if node.is_leaf else "N",
            paths_str,
            Path(node.file_path).name if node.file_path else "",
        )

    if len(result) == 0:
        console.print(
            f"[yellow]No {direction_label.lower()} found for '{result.query_column}'[/yellow]"
        )
    else:
        console.print(table)
        console.print(f"\n[dim]Total: {len(result)} column(s)[/dim]")


def _format_query_result_json(result) -> None:
    """Format query result as JSON."""
    columns = []
    for node in result.related_columns:
        node_data = node.model_dump()
        # Serialize paths as arrays of node identifiers for cleaner output
        node_data["paths"] = [p.nodes for p in node.paths]
        columns.append(node_data)

    output = {
        "query_column": result.query_column,
        "direction": result.direction,
        "count": len(result),
        "columns": columns,
    }
    print(json.dumps(output, indent=2))


def _format_query_result_csv(result) -> None:
    """Format query result as CSV."""
    print(
        "identifier,table,column,hops,output_column,is_root,is_leaf,paths,file_path,query_index"
    )
    for node in result.related_columns:
        file_path = node.file_path.replace('"', '""') if node.file_path else ""
        # Format paths as semicolon-separated arrow strings
        paths_str = (
            ";".join(p.to_arrow_string() for p in node.paths) if node.paths else ""
        )
        paths_str = paths_str.replace('"', '""')

        print(
            f'"{node.identifier}","{node.table or ""}","{node.column or ""}",'
            f'{node.hops},"{node.output_column}",'
            f"{'true' if node.is_root else 'false'},"
            f"{'true' if node.is_leaf else 'false'},"
            f'"{paths_str}","{file_path}",{node.query_index}'
        )


@app.command()
def dissect(
    sql_file: Annotated[
        typer.FileText,
        typer.Argument(
            default_factory=lambda: sys.stdin,
            show_default="stdin",
            help="Path to SQL file to dissect (reads from stdin if not provided)",
        ),
    ],
    dialect: Optional[str] = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect (default: spark, or from config)",
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
    templater: Optional[str] = typer.Option(
        None,
        "--templater",
        "-t",
        help="Templater for SQL preprocessing (e.g., 'jinja', 'none')",
    ),
    var: Optional[List[str]] = typer.Option(
        None,
        "--var",
        "-v",
        help="Template variable in key=value format (repeatable)",
    ),
    vars_file: Optional[Path] = typer.Option(
        None,
        "--vars-file",
        exists=True,
        help="Path to variables file (JSON or YAML)",
    ),
) -> None:
    """
    Dissect SQL queries into constituent components.

    Extracts CTEs, subqueries, main query, DML targets, source SELECTs,
    UNION branches, and scalar subqueries for analysis and unit testing.

    Configuration can be set in sqlglider.toml in the current directory.
    CLI arguments override configuration file values.

    Examples:

        # Dissect a SQL file
        sqlglider dissect query.sql

        # Export to JSON format
        sqlglider dissect query.sql --output-format json

        # Export to CSV file
        sqlglider dissect query.sql --output-format csv --output-file dissected.csv

        # Use different SQL dialect
        sqlglider dissect query.sql --dialect postgres

        # Dissect templated SQL with Jinja2
        sqlglider dissect query.sql --templater jinja --var schema=analytics
    """
    from sqlglider.dissection.analyzer import DissectionAnalyzer
    from sqlglider.dissection.formatters import (
        DissectionCsvFormatter,
        DissectionJsonFormatter,
        DissectionTextFormatter,
    )
    from sqlglider.dissection.formatters import (
        OutputWriter as DissectionOutputWriter,
    )

    # Load configuration from sqlglider.toml (if it exists)
    config = load_config()

    # Apply priority resolution: CLI args > config > defaults
    dialect = dialect or config.dialect or "spark"
    output_format = output_format or config.output_format or "text"
    templater = templater or config.templater  # None means no templating

    # Validate output format
    if output_format not in ["text", "json", "csv"]:
        err_console.print(
            f"[red]Error:[/red] Invalid output format '{output_format}'. "
            "Use 'text', 'json', or 'csv'."
        )
        raise typer.Exit(1)

    # Check if reading from stdin
    is_stdin = sql_file.name == "<stdin>"

    try:
        # Check if stdin is being used without input
        if is_stdin and sys.stdin.isatty():
            err_console.print(
                "[red]Error:[/red] No SQL file provided and stdin is interactive. "
                "Provide a SQL file path or pipe SQL via stdin."
            )
            raise typer.Exit(1)

        # Read SQL from file or stdin
        sql = sql_file.read()

        # Determine source path for templating (None if stdin)
        source_path = None if is_stdin else Path(sql_file.name)

        # Apply templating if specified
        sql = _apply_templating(
            sql,
            templater_name=templater,
            cli_vars=var,
            vars_file=vars_file,
            config=config,
            source_path=source_path,
        )

        # Create analyzer
        analyzer = DissectionAnalyzer(sql, dialect=dialect)

        # Dissect queries
        results = analyzer.dissect_queries()

        # Format and output based on output format
        if output_format == "text":
            if output_file:
                # For file output, use a string-based console to capture output
                from io import StringIO

                from rich.console import Console as FileConsole

                string_buffer = StringIO()
                file_console = FileConsole(file=string_buffer, force_terminal=False)
                DissectionTextFormatter.format(results, file_console)
                output_file.write_text(string_buffer.getvalue(), encoding="utf-8")
                console.print(
                    f"[green]Success:[/green] Dissection written to {output_file}"
                )
            else:
                # Direct console output with Rich formatting
                DissectionTextFormatter.format(results, console)
        elif output_format == "json":
            formatted = DissectionJsonFormatter.format(results)
            DissectionOutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Dissection written to {output_file}"
                )
        else:  # csv
            formatted = DissectionCsvFormatter.format(results)
            DissectionOutputWriter.write(formatted, output_file)
            if output_file:
                console.print(
                    f"[green]Success:[/green] Dissection written to {output_file}"
                )

    except FileNotFoundError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ParseError as e:
        err_console.print(f"[red]Error:[/red] Failed to parse SQL: {e}")
        raise typer.Exit(1)

    except TemplaterError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except ValueError as e:
        err_console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    except Exception as e:
        err_console.print(f"[red]Error:[/red] Unexpected error: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
