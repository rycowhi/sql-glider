"""Shared schema extraction logic for inferring table schemas from SQL files."""

from pathlib import Path
from typing import Callable, Dict, List, Optional

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from sqlglider.lineage.analyzer import LineageAnalyzer, SchemaResolutionError
from sqlglider.utils.file_utils import read_sql_file
from sqlglider.utils.schema import parse_ddl_to_schema

SchemaDict = Dict[str, Dict[str, str]]
SqlPreprocessor = Callable[[str, Path], str]


def extract_schemas_from_files(
    file_paths: List[Path],
    dialect: str = "spark",
    sql_preprocessor: Optional[SqlPreprocessor] = None,
    initial_schema: Optional[SchemaDict] = None,
    strict_schema: bool = False,
    console: Optional[Console] = None,
) -> SchemaDict:
    """Extract schema from SQL files by parsing DDL and inferring from DQL.

    Iterates through files, accumulating schema knowledge. Each file's
    inferred schema is available when parsing subsequent files.

    Args:
        file_paths: SQL files to extract schema from.
        dialect: SQL dialect.
        sql_preprocessor: Optional function to preprocess SQL (e.g., templating).
        initial_schema: Optional starting schema to build upon.
        strict_schema: If True, fail on ambiguous column attribution.
        console: Rich console for output. Uses stderr if not provided.

    Returns:
        Accumulated schema dict mapping table names to column dicts.
    """
    if console is None:
        console = Console(stderr=True)

    schema: SchemaDict = (
        {
            k.lower(): {c.lower(): v for c, v in cols.items()}
            for k, cols in initial_schema.items()
        }
        if initial_schema
        else {}
    )
    total = len(file_paths)

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task("Extracting schema", total=total)
        for i, file_path in enumerate(file_paths, start=1):
            console.print(f"Extracting schema {i}/{total}: {file_path.name}")
            try:
                sql_content = read_sql_file(file_path)
                if sql_preprocessor:
                    sql_content = sql_preprocessor(sql_content, file_path)
                analyzer = LineageAnalyzer(
                    sql_content,
                    dialect=dialect,
                    schema=schema,
                    strict_schema=strict_schema,
                )
                file_schema = analyzer.extract_schema_only()
                for table_name, columns in file_schema.items():
                    if table_name in schema:
                        schema[table_name].update(columns)
                    else:
                        schema[table_name] = columns
            except SchemaResolutionError:
                raise
            except Exception:
                # Schema extraction failures are non-fatal; the file
                # will be reported during the lineage pass if it also fails.
                pass
            progress.advance(task)
    return schema


def fill_schema_from_catalog(
    schema: SchemaDict,
    file_paths: List[Path],
    dialect: str = "spark",
    sql_preprocessor: Optional[SqlPreprocessor] = None,
    catalog_type: str = "databricks",
    catalog_config: Optional[Dict[str, object]] = None,
    console: Optional[Console] = None,
) -> SchemaDict:
    """Pull DDL from catalog for tables not yet in schema.

    Args:
        schema: Schema dict already populated from file extraction.
        file_paths: SQL files to scan for table references.
        dialect: SQL dialect.
        sql_preprocessor: Optional SQL preprocessor.
        catalog_type: Catalog provider name.
        catalog_config: Provider-specific configuration dict.
        console: Rich console for output.

    Returns:
        Updated schema dict with catalog-sourced entries added.
    """
    from sqlglider.catalog import get_catalog
    from sqlglider.lineage.analyzer import ObjectType

    if console is None:
        console = Console(stderr=True)

    catalog = get_catalog(catalog_type)
    if catalog_config:
        catalog.configure(catalog_config)

    # Collect all referenced table names across files
    all_tables: set[str] = set()
    for file_path in file_paths:
        try:
            sql_content = read_sql_file(file_path)
            if sql_preprocessor:
                sql_content = sql_preprocessor(sql_content, file_path)
            analyzer = LineageAnalyzer(sql_content, dialect=dialect)
            tables_results = analyzer.analyze_tables()
            for result in tables_results:
                for table_info in result.tables:
                    if table_info.object_type != ObjectType.CTE:
                        all_tables.add(table_info.name)
        except Exception:
            pass

    # Find tables missing from schema
    missing = [t for t in all_tables if t not in schema]
    if not missing:
        return schema

    console.print(
        f"[blue]Pulling DDL from {catalog_type} for {len(missing)} table(s)...[/blue]"
    )

    ddl_results = catalog.get_ddl_batch(missing)
    for table_name, ddl in ddl_results.items():
        if ddl.startswith("ERROR:"):
            console.print(
                f"[yellow]Warning:[/yellow] Could not pull DDL for {table_name}: {ddl}"
            )
            continue
        parsed_schema = parse_ddl_to_schema(ddl, dialect=dialect)
        for name, cols in parsed_schema.items():
            if name not in schema:
                schema[name] = cols

    return schema


def extract_and_resolve_schema(
    file_paths: List[Path],
    dialect: str = "spark",
    sql_preprocessor: Optional[SqlPreprocessor] = None,
    initial_schema: Optional[SchemaDict] = None,
    strict_schema: bool = False,
    catalog_type: Optional[str] = None,
    catalog_config: Optional[Dict[str, object]] = None,
    console: Optional[Console] = None,
) -> SchemaDict:
    """Extract schema from files and optionally fill from catalog.

    High-level orchestrator that runs file-based extraction followed
    by optional catalog resolution.

    Args:
        file_paths: SQL files to extract schema from.
        dialect: SQL dialect.
        sql_preprocessor: Optional SQL preprocessor.
        initial_schema: Optional starting schema to build upon.
        strict_schema: If True, fail on ambiguous column attribution.
        catalog_type: Optional catalog provider name.
        catalog_config: Optional provider-specific configuration dict.
        console: Rich console for output.

    Returns:
        Resolved schema dict.
    """
    if console is None:
        console = Console(stderr=True)

    console.print("[blue]Extracting schema from files[/blue]")
    schema = extract_schemas_from_files(
        file_paths,
        dialect=dialect,
        sql_preprocessor=sql_preprocessor,
        initial_schema=initial_schema,
        strict_schema=strict_schema,
        console=console,
    )

    if catalog_type:
        schema = fill_schema_from_catalog(
            schema,
            file_paths,
            dialect=dialect,
            sql_preprocessor=sql_preprocessor,
            catalog_type=catalog_type,
            catalog_config=catalog_config,
            console=console,
        )

    console.print(f"[blue]Schema resolved for {len(schema)} table(s)[/blue]")
    return schema
