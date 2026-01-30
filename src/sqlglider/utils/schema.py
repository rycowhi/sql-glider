"""Schema utilities for parsing DDL into schema dictionaries."""

from typing import Dict

from sqlglot import exp, parse


def parse_ddl_to_schema(ddl: str, dialect: str = "spark") -> Dict[str, Dict[str, str]]:
    """Extract table schemas from DDL statements.

    Parses CREATE TABLE/VIEW statements and extracts column names.
    Only column names are needed — types are stored as "UNKNOWN" since
    SQLGlot's lineage only uses names for star expansion.

    Args:
        ddl: SQL string containing one or more CREATE TABLE/VIEW statements
        dialect: SQL dialect for parsing

    Returns:
        Schema dict mapping table names to column definitions,
        e.g. {"my_table": {"id": "UNKNOWN", "name": "UNKNOWN"}}
    """
    schema: Dict[str, Dict[str, str]] = {}
    expressions = parse(ddl, dialect=dialect)

    for expr in expressions:
        if expr is None:
            continue
        if not isinstance(expr, (exp.Create,)):
            continue

        # Get target table name
        target = expr.this
        if isinstance(target, exp.Schema):
            # Schema node wraps the table and column definitions
            columns = [
                col.name for col in target.expressions if isinstance(col, exp.ColumnDef)
            ]
            target = target.this
        else:
            columns = []

        if not isinstance(target, exp.Table):
            continue

        table_name = _get_qualified_name(target)

        if columns:
            schema[table_name] = {col.lower(): "UNKNOWN" for col in columns}

    return schema


def _get_qualified_name(table: exp.Table) -> str:
    """Build a qualified table name from a SQLGlot Table expression."""
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    parts.append(table.name)
    return ".".join(parts).lower()
