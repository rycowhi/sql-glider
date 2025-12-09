"""Core lineage analysis using SQLGlot."""

from enum import Enum
from typing import Callable, Iterator, Literal, List, Optional, Set, Tuple

from pydantic import BaseModel, Field
from sqlglot import exp, parse
from sqlglot.errors import ParseError
from sqlglot.lineage import Node, lineage


class TableUsage(str, Enum):
    """How a table is used in a query."""

    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    BOTH = "BOTH"


class ObjectType(str, Enum):
    """Type of database object."""

    TABLE = "TABLE"
    VIEW = "VIEW"
    CTE = "CTE"
    UNKNOWN = "UNKNOWN"


class TableInfo(BaseModel):
    """Information about a table referenced in a query."""

    name: str = Field(..., description="Fully qualified table name")
    usage: TableUsage = Field(
        ..., description="How the table is used (INPUT, OUTPUT, BOTH)"
    )
    object_type: ObjectType = Field(
        ..., description="Type of object (TABLE, VIEW, CTE, UNKNOWN)"
    )


class QueryTablesResult(BaseModel):
    """Result of table analysis for a single query."""

    metadata: "QueryMetadata"
    tables: List[TableInfo] = Field(default_factory=list)


class LineageItem(BaseModel):
    """Represents a single lineage relationship (output -> source)."""

    output_name: str = Field(..., description="Output column/table name")
    source_name: str = Field(..., description="Source column/table name")


class QueryMetadata(BaseModel):
    """Query execution context."""

    query_index: int = Field(..., description="0-based query index")
    query_preview: str = Field(..., description="First 100 chars of query")


class QueryLineageResult(BaseModel):
    """Complete lineage result for a single query."""

    metadata: QueryMetadata
    lineage_items: List[LineageItem] = Field(default_factory=list)
    level: Literal["column", "table"]


class SkippedQuery(BaseModel):
    """Information about a query that was skipped during analysis."""

    query_index: int = Field(..., description="0-based query index")
    statement_type: str = Field(..., description="Type of SQL statement (e.g., CREATE)")
    reason: str = Field(..., description="Reason for skipping")
    query_preview: str = Field(..., description="First 100 chars of query")


# Type alias for warning callback function
WarningCallback = Callable[[str], None]


class LineageAnalyzer:
    """Analyze column and table lineage for SQL queries."""

    def __init__(self, sql: str, dialect: str = "spark"):
        """
        Initialize the lineage analyzer.

        Args:
            sql: SQL query string to analyze (can contain multiple statements)
            dialect: SQL dialect (default: spark)

        Raises:
            ParseError: If the SQL cannot be parsed
        """
        self.sql = sql
        self.dialect = dialect
        self._skipped_queries: List[SkippedQuery] = []

        try:
            # Parse all statements in the SQL string
            self.expressions = parse(sql, dialect=dialect)

            # Filter out None values (can happen with empty statements or comments)
            self.expressions = [expr for expr in self.expressions if expr is not None]

            if not self.expressions:
                raise ParseError("No valid SQL statements found")

            # For backward compatibility, store first expression as self.expr
            self.expr = self.expressions[0]

        except ParseError as e:
            raise ParseError(f"Invalid SQL syntax: {e}") from e

    @property
    def skipped_queries(self) -> List[SkippedQuery]:
        """Get list of queries that were skipped during analysis."""
        return self._skipped_queries.copy()

    def get_output_columns(self) -> List[str]:
        """
        Extract all output column names from the query with full qualification.

        For DML/DDL statements (INSERT, UPDATE, MERGE, CREATE TABLE AS, etc.),
        returns the target table columns. For DQL (SELECT), returns the selected columns.

        Returns:
            List of fully qualified output column names (table.column or database.table.column)

        Raises:
            ValueError: If the statement type is not supported for lineage analysis
        """
        columns = []

        # Build mapping for qualified names
        self._column_mapping = {}  # Maps qualified name -> lineage column name

        # Check if this is a DML/DDL statement
        result = self._get_target_and_select()
        if result is None:
            # Unsupported statement type
            stmt_type = self._get_statement_type()
            raise ValueError(
                f"Statement type '{stmt_type}' does not support lineage analysis"
            )

        target_table, select_node = result

        if target_table:
            # DML/DDL: Use target table for output column qualification
            # The columns are from the SELECT, but qualified with the target table
            for projection in select_node.expressions:
                # Get the underlying expression (unwrap alias if present)
                if isinstance(projection, exp.Alias):
                    # For aliased columns, use the alias as the column name
                    column_name = projection.alias
                    lineage_name = column_name  # SQLGlot lineage uses the alias
                else:
                    source_expr = projection
                    if isinstance(source_expr, exp.Column):
                        column_name = source_expr.name
                        lineage_name = column_name
                    else:
                        # For expressions, use the SQL representation
                        column_name = source_expr.sql(dialect=self.dialect)
                        lineage_name = column_name

                # Qualify with target table
                qualified_name = f"{target_table}.{column_name}"
                columns.append(qualified_name)
                self._column_mapping[qualified_name] = lineage_name

        else:
            # DQL (pure SELECT): Use the SELECT columns as output
            for projection in select_node.expressions:
                # Get the underlying expression (unwrap alias if present)
                if isinstance(projection, exp.Alias):
                    source_expr = projection.this
                    column_name = projection.alias
                    lineage_name = column_name  # SQLGlot lineage uses the alias
                else:
                    source_expr = projection
                    column_name = None
                    lineage_name = None

                # Try to extract fully qualified name
                if isinstance(source_expr, exp.Column):
                    # Get table and column parts
                    table_name = source_expr.table
                    col_name = column_name or source_expr.name

                    if table_name:
                        # Resolve table reference (could be table, CTE, or subquery alias)
                        # This works at any nesting level because we're only looking at the immediate context
                        resolved_table = self._resolve_table_reference(
                            table_name, select_node
                        )
                        qualified_name = f"{resolved_table}.{col_name}"
                        columns.append(qualified_name)
                        # Map qualified name to what lineage expects
                        self._column_mapping[qualified_name] = lineage_name or col_name
                    else:
                        # No table qualifier - try to infer from FROM clause
                        # This handles "SELECT col FROM single_source" cases
                        inferred_table = self._infer_single_table_source(select_node)
                        if inferred_table:
                            qualified_name = f"{inferred_table}.{col_name}"
                            columns.append(qualified_name)
                            self._column_mapping[qualified_name] = (
                                lineage_name or col_name
                            )
                        else:
                            # Can't infer table, just use column name
                            columns.append(col_name)
                            self._column_mapping[col_name] = lineage_name or col_name
                else:
                    # For other expressions (literals, functions, etc.)
                    # Use the alias if available, otherwise the SQL representation
                    if column_name:
                        columns.append(column_name)
                        self._column_mapping[column_name] = column_name
                    else:
                        expr_str = source_expr.sql(dialect=self.dialect)
                        columns.append(expr_str)
                        self._column_mapping[expr_str] = expr_str

        return columns

    def analyze_queries(
        self,
        level: Literal["column", "table"] = "column",
        column: Optional[str] = None,
        source_column: Optional[str] = None,
        table_filter: Optional[str] = None,
    ) -> List[QueryLineageResult]:
        """
        Unified lineage analysis for single or multi-query files.

        This method replaces all previous analysis methods (analyze_column_lineage,
        analyze_reverse_lineage, analyze_table_lineage, analyze_all_queries, etc.)
        with a single unified interface.

        Args:
            level: Analysis level ("column" or "table")
            column: Target output column for forward lineage
            source_column: Source column for reverse lineage (impact analysis)
            table_filter: Filter queries to those referencing this table

        Returns:
            List of QueryLineageResult objects (one per query that matches filters)

        Raises:
            ValueError: If column or source_column is specified but not found

        Examples:
            # Forward lineage for all columns
            results = analyzer.analyze_queries(level="column")

            # Forward lineage for specific column
            results = analyzer.analyze_queries(level="column", column="customers.id")

            # Reverse lineage (impact analysis)
            results = analyzer.analyze_queries(level="column", source_column="orders.customer_id")

            # Table-level lineage
            results = analyzer.analyze_queries(level="table")

            # Filter by table (multi-query files)
            results = analyzer.analyze_queries(table_filter="customers")
        """
        results = []
        self._skipped_queries = []  # Reset skipped queries for this analysis

        for query_index, expr, preview in self._iterate_queries(table_filter):
            # Temporarily swap self.expr to analyze this query
            original_expr = self.expr
            self.expr = expr

            try:
                lineage_items: List[LineageItem] = []

                if level == "column":
                    if source_column:
                        # Reverse lineage (impact analysis)
                        lineage_items = self._analyze_reverse_lineage_internal(
                            source_column
                        )
                        if not lineage_items:
                            # Source column not found in this query - skip it
                            continue
                    else:
                        # Forward lineage
                        lineage_items = self._analyze_column_lineage_internal(column)
                        if not lineage_items:
                            # Column not found in this query (if column was specified) - skip it
                            if column:
                                continue
                else:  # table
                    lineage_items = self._analyze_table_lineage_internal()

                # Create query result
                results.append(
                    QueryLineageResult(
                        metadata=QueryMetadata(
                            query_index=query_index,
                            query_preview=preview,
                        ),
                        lineage_items=lineage_items,
                        level=level,
                    )
                )
            except ValueError as e:
                # Unsupported statement type - track it and continue
                stmt_type = self._get_statement_type(expr)
                self._skipped_queries.append(
                    SkippedQuery(
                        query_index=query_index,
                        statement_type=stmt_type,
                        reason=str(e),
                        query_preview=preview,
                    )
                )
            finally:
                # Restore original expression
                self.expr = original_expr

        # Validate: if a specific column or source_column was specified and we got no results,
        # raise ValueError to preserve backward compatibility
        if not results:
            if column:
                raise ValueError(
                    f"Column '{column}' not found in any query. "
                    "Please check the column name and try again."
                )
            elif source_column:
                raise ValueError(
                    f"Source column '{source_column}' not found in any query. "
                    "Please check the column name and try again."
                )

        return results

    def analyze_tables(
        self,
        table_filter: Optional[str] = None,
    ) -> List[QueryTablesResult]:
        """
        Analyze all tables involved in SQL queries.

        This method extracts information about all tables referenced in the SQL,
        including their usage (INPUT, OUTPUT, or BOTH) and object type (TABLE, VIEW,
        CTE, or UNKNOWN).

        Args:
            table_filter: Filter queries to those referencing this table

        Returns:
            List of QueryTablesResult objects (one per query that matches filters)

        Examples:
            # Get all tables from SQL
            results = analyzer.analyze_tables()

            # Filter by table (multi-query files)
            results = analyzer.analyze_tables(table_filter="customers")
        """
        results = []

        for query_index, expr, preview in self._iterate_queries(table_filter):
            # Temporarily swap self.expr to analyze this query
            original_expr = self.expr
            self.expr = expr

            try:
                tables = self._extract_tables_from_query()

                # Create query result
                results.append(
                    QueryTablesResult(
                        metadata=QueryMetadata(
                            query_index=query_index,
                            query_preview=preview,
                        ),
                        tables=tables,
                    )
                )
            finally:
                # Restore original expression
                self.expr = original_expr

        return results

    def _extract_tables_from_query(self) -> List[TableInfo]:
        """
        Extract all tables from the current query with usage and type information.

        Returns:
            List of TableInfo objects for all tables in the query.
        """
        # Track tables by name to consolidate INPUT/OUTPUT into BOTH
        tables_dict: dict[str, TableInfo] = {}

        # Extract CTEs first (they're INPUT only)
        cte_names = self._extract_cte_names()
        for cte_name in cte_names:
            tables_dict[cte_name] = TableInfo(
                name=cte_name,
                usage=TableUsage.INPUT,
                object_type=ObjectType.CTE,
            )

        # Determine target table and its type based on statement type
        target_table, target_type = self._get_target_table_info()

        # Get all table references in the query (except CTEs)
        input_tables = self._get_all_input_tables(cte_names)

        # Add target table as OUTPUT
        if target_table:
            if target_table in tables_dict:
                # Table is both input and output (e.g., UPDATE with self-reference)
                tables_dict[target_table] = TableInfo(
                    name=target_table,
                    usage=TableUsage.BOTH,
                    object_type=target_type,
                )
            else:
                tables_dict[target_table] = TableInfo(
                    name=target_table,
                    usage=TableUsage.OUTPUT,
                    object_type=target_type,
                )

        # Add input tables
        for table_name in input_tables:
            if table_name in tables_dict:
                # Already exists - might need to upgrade to BOTH
                existing = tables_dict[table_name]
                if existing.usage == TableUsage.OUTPUT:
                    tables_dict[table_name] = TableInfo(
                        name=table_name,
                        usage=TableUsage.BOTH,
                        object_type=existing.object_type,
                    )
                # If INPUT or BOTH, keep as-is
            else:
                tables_dict[table_name] = TableInfo(
                    name=table_name,
                    usage=TableUsage.INPUT,
                    object_type=ObjectType.UNKNOWN,
                )

        # Return sorted list by name for consistent output
        return sorted(tables_dict.values(), key=lambda t: t.name.lower())

    def _extract_cte_names(self) -> Set[str]:
        """
        Extract all CTE (Common Table Expression) names from the query.

        Returns:
            Set of CTE names defined in the WITH clause.
        """
        cte_names: Set[str] = set()

        # Look for WITH clause
        if hasattr(self.expr, "args") and self.expr.args.get("with"):
            with_clause = self.expr.args["with"]
            for cte in with_clause.expressions:
                if isinstance(cte, exp.CTE) and cte.alias:
                    cte_names.add(cte.alias)

        return cte_names

    def _get_target_table_info(self) -> Tuple[Optional[str], ObjectType]:
        """
        Get the target table name and its object type for DML/DDL statements.

        Returns:
            Tuple of (target_table_name, object_type) or (None, UNKNOWN) for SELECT.
        """
        # INSERT INTO table
        if isinstance(self.expr, exp.Insert):
            target = self.expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), ObjectType.UNKNOWN)

        # CREATE TABLE / CREATE VIEW
        elif isinstance(self.expr, exp.Create):
            kind = getattr(self.expr, "kind", "").upper()
            target = self.expr.this

            # Handle Schema wrapper (CREATE TABLE with columns)
            if isinstance(target, exp.Schema):
                target = target.this

            if isinstance(target, exp.Table):
                table_name = self._get_qualified_table_name(target)
                if kind == "VIEW":
                    return (table_name, ObjectType.VIEW)
                elif kind == "TABLE":
                    return (table_name, ObjectType.TABLE)
                else:
                    return (table_name, ObjectType.UNKNOWN)

        # UPDATE table
        elif isinstance(self.expr, exp.Update):
            target = self.expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), ObjectType.UNKNOWN)

        # MERGE INTO table
        elif isinstance(self.expr, exp.Merge):
            target = self.expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), ObjectType.UNKNOWN)

        # DELETE FROM table
        elif isinstance(self.expr, exp.Delete):
            target = self.expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), ObjectType.UNKNOWN)

        # DROP TABLE / DROP VIEW
        elif isinstance(self.expr, exp.Drop):
            kind = getattr(self.expr, "kind", "").upper()
            target = self.expr.this
            if isinstance(target, exp.Table):
                table_name = self._get_qualified_table_name(target)
                if kind == "VIEW":
                    return (table_name, ObjectType.VIEW)
                elif kind == "TABLE":
                    return (table_name, ObjectType.TABLE)
                else:
                    return (table_name, ObjectType.UNKNOWN)

        # SELECT (no target table)
        return (None, ObjectType.UNKNOWN)

    def _get_all_input_tables(self, exclude_ctes: Set[str]) -> Set[str]:
        """
        Get all tables used as input (FROM, JOIN, subqueries, etc.).

        Args:
            exclude_ctes: Set of CTE names to exclude from results.

        Returns:
            Set of fully qualified table names that are used as input.
        """
        input_tables: Set[str] = set()

        # Find all Table nodes in the expression tree
        for table_node in self.expr.find_all(exp.Table):
            table_name = self._get_qualified_table_name(table_node)

            # Skip CTEs (they're tracked separately)
            if table_name in exclude_ctes:
                continue

            # Skip the target table for certain statement types
            # (it will be added separately as OUTPUT)
            if self._is_target_table(table_node):
                continue

            input_tables.add(table_name)

        return input_tables

    def _is_target_table(self, table_node: exp.Table) -> bool:
        """
        Check if a table node is the target of a DML/DDL statement.

        This helps distinguish the target table (OUTPUT) from source tables (INPUT)
        in statements like INSERT, UPDATE, MERGE, DELETE.

        Args:
            table_node: The table node to check.

        Returns:
            True if this is the target table, False otherwise.
        """
        # For INSERT, the target is self.expr.this
        if isinstance(self.expr, exp.Insert):
            return table_node is self.expr.this

        # For UPDATE, the target is self.expr.this
        elif isinstance(self.expr, exp.Update):
            return table_node is self.expr.this

        # For MERGE, the target is self.expr.this
        elif isinstance(self.expr, exp.Merge):
            return table_node is self.expr.this

        # For DELETE, the target is self.expr.this
        elif isinstance(self.expr, exp.Delete):
            return table_node is self.expr.this

        # For CREATE TABLE/VIEW, check if it's in the schema
        elif isinstance(self.expr, exp.Create):
            target = self.expr.this
            if isinstance(target, exp.Schema):
                return table_node is target.this
            return table_node is target

        # For DROP, the target is self.expr.this
        elif isinstance(self.expr, exp.Drop):
            return table_node is self.expr.this

        return False

    def _analyze_column_lineage_internal(
        self, column: Optional[str] = None
    ) -> List[LineageItem]:
        """
        Internal method for analyzing column lineage. Returns flat list of LineageItem.

        Args:
            column: Optional specific column to analyze. If None, analyzes all columns.

        Returns:
            List of LineageItem objects (one per output-source relationship)
        """
        output_columns = self.get_output_columns()

        if column:
            # Analyze only the specified column (case-insensitive matching)
            matched_column = None
            column_lower = column.lower()
            for output_col in output_columns:
                if output_col.lower() == column_lower:
                    matched_column = output_col
                    break

            if matched_column is None:
                # Column not found - return empty list (caller will skip this query)
                return []
            columns_to_analyze = [matched_column]
        else:
            # Analyze all columns
            columns_to_analyze = output_columns

        lineage_items = []
        # Get SQL for current expression only (not full multi-query SQL)
        current_query_sql = self.expr.sql(dialect=self.dialect)

        for col in columns_to_analyze:
            try:
                # Get the column name that lineage expects
                lineage_col = self._column_mapping.get(col, col)

                # Get lineage tree for this column using current query SQL only
                node = lineage(lineage_col, current_query_sql, dialect=self.dialect)

                # Collect all source columns
                sources: Set[str] = set()
                self._collect_source_columns(node, sources)

                # Convert to flat LineageItem list (one item per source)
                for source in sorted(sources):
                    lineage_items.append(
                        LineageItem(output_name=col, source_name=source)
                    )

                # If no sources found, add single item with empty source
                if not sources:
                    lineage_items.append(LineageItem(output_name=col, source_name=""))
            except Exception:
                # If lineage fails for a column, add item with empty source
                lineage_items.append(LineageItem(output_name=col, source_name=""))

        return lineage_items

    def _analyze_table_lineage_internal(self) -> List[LineageItem]:
        """
        Internal method for analyzing table lineage. Returns flat list of LineageItem.

        Returns:
            List of LineageItem objects (one per output-source table relationship)
        """
        source_tables: Set[str] = set()

        # Find all Table nodes in the AST
        for table_node in self.expr.find_all(exp.Table):
            # Get fully qualified table name
            table_name = table_node.sql(dialect=self.dialect)
            source_tables.add(table_name)

        # The output table would typically be defined in INSERT/CREATE statements
        # For SELECT statements, we use a placeholder
        output_table = "query_result"

        # Convert to flat LineageItem list (one item per source table)
        lineage_items = []
        for source in sorted(source_tables):
            lineage_items.append(
                LineageItem(output_name=output_table, source_name=source)
            )

        return lineage_items

    def _analyze_reverse_lineage_internal(
        self, source_column: str
    ) -> List[LineageItem]:
        """
        Internal method for analyzing reverse lineage. Returns flat list of LineageItem.

        Args:
            source_column: Source column to analyze (e.g., "orders.customer_id")

        Returns:
            List of LineageItem objects (source column -> affected outputs)
        """
        # Step 1: Run forward lineage on all output columns
        forward_items = self._analyze_column_lineage_internal(column=None)

        # Step 2: Build reverse mapping (source -> [affected outputs])
        reverse_map: dict[str, set[str]] = {}
        all_outputs = set()

        for item in forward_items:
            all_outputs.add(item.output_name)
            if item.source_name:  # Skip empty sources
                if item.source_name not in reverse_map:
                    reverse_map[item.source_name] = set()
                reverse_map[item.source_name].add(item.output_name)

        # Step 3: Find matching source (case-insensitive)
        matched_source = None
        affected_outputs = set()
        source_column_lower = source_column.lower()

        # First check if it's in reverse_map (derived columns)
        for source in reverse_map.keys():
            if source.lower() == source_column_lower:
                matched_source = source
                affected_outputs = reverse_map[matched_source]
                break

        # If not found, check if it's an output column (base table column)
        if matched_source is None:
            for output in all_outputs:
                if output.lower() == source_column_lower:
                    matched_source = output
                    affected_outputs = {output}  # It affects itself
                    break

        if matched_source is None:
            # Source column not found - return empty list (caller will skip this query)
            return []

        # Step 4: Return with semantic swap (source as output, affected as sources)
        # This maintains the LineageItem structure where output_name is what we're looking at
        # and source_name is what it affects
        lineage_items = []
        for affected in sorted(affected_outputs):
            lineage_items.append(
                LineageItem(output_name=matched_source, source_name=affected)
            )

        return lineage_items

    def _get_statement_type(self, expr: Optional[exp.Expression] = None) -> str:
        """
        Get a human-readable name for the SQL statement type.

        Args:
            expr: Expression to check (uses self.expr if not provided)

        Returns:
            Statement type name (e.g., "CREATE FUNCTION", "SELECT", "DELETE")
        """
        target_expr = expr if expr is not None else self.expr
        expr_type = type(target_expr).__name__

        # Map common expression types to more readable names
        type_map = {
            "Select": "SELECT",
            "Insert": "INSERT",
            "Update": "UPDATE",
            "Delete": "DELETE",
            "Merge": "MERGE",
            "Create": f"CREATE {getattr(target_expr, 'kind', '')}".strip(),
            "Drop": f"DROP {getattr(target_expr, 'kind', '')}".strip(),
            "Alter": "ALTER",
            "Truncate": "TRUNCATE",
            "Command": "COMMAND",
        }

        return type_map.get(expr_type, expr_type.upper())

    def _get_target_and_select(
        self,
    ) -> Optional[tuple[Optional[str], exp.Select]]:
        """
        Detect if this is a DML/DDL statement and extract the target table and SELECT node.

        Returns:
            Tuple of (target_table_name, select_node) where:
            - target_table_name is the fully qualified target table for DML/DDL, or None for pure SELECT
            - select_node is the SELECT statement that provides the data
            - Returns None if the statement type doesn't contain a SELECT (e.g., CREATE FUNCTION)

        Handles:
        - INSERT INTO table SELECT ...
        - CREATE TABLE table AS SELECT ...
        - MERGE INTO table ...
        - UPDATE table SET ... FROM (SELECT ...)
        - Pure SELECT (returns None as target)
        """
        # Check for INSERT statement
        if isinstance(self.expr, exp.Insert):
            target = self.expr.this
            if isinstance(target, exp.Table):
                target_name = self._get_qualified_table_name(target)
                # Find the SELECT within the INSERT
                select_node = self.expr.expression
                if isinstance(select_node, exp.Select):
                    return (target_name, select_node)

        # Check for CREATE TABLE AS SELECT (CTAS) or CREATE VIEW AS SELECT
        elif isinstance(self.expr, exp.Create):
            if self.expr.kind in ("TABLE", "VIEW"):
                target = self.expr.this
                if isinstance(target, exp.Schema):
                    # Get the table from schema
                    target = target.this
                if isinstance(target, exp.Table):
                    target_name = self._get_qualified_table_name(target)
                    # Find the SELECT in the expression
                    select_node = self.expr.expression
                    if isinstance(select_node, exp.Select):
                        return (target_name, select_node)

        # Check for MERGE statement
        elif isinstance(self.expr, exp.Merge):
            target = self.expr.this
            if isinstance(target, exp.Table):
                target_name = self._get_qualified_table_name(target)
                # For MERGE, we need to find the SELECT in the USING clause
                # This is more complex, for now treat it as a SELECT
                select_nodes = list(self.expr.find_all(exp.Select))
                if select_nodes:
                    return (target_name, select_nodes[0])

        # Check for UPDATE with subquery
        elif isinstance(self.expr, exp.Update):
            target = self.expr.this
            if isinstance(target, exp.Table):
                target_name = self._get_qualified_table_name(target)
                # For UPDATE, find the SELECT if there is one
                select_nodes = list(self.expr.find_all(exp.Select))
                if select_nodes:
                    return (target_name, select_nodes[0])

        # Default: Pure SELECT (DQL)
        select_nodes = list(self.expr.find_all(exp.Select))
        if select_nodes:
            return (None, select_nodes[0])

        # Fallback: return the expression as-is if it's a SELECT
        if isinstance(self.expr, exp.Select):
            return (None, self.expr)

        # No SELECT found - return None to indicate unsupported statement
        return None

    def _get_qualified_table_name(self, table: exp.Table) -> str:
        """
        Get the fully qualified name for a table.

        Args:
            table: SQLGlot Table expression

        Returns:
            Fully qualified table name (database.table or catalog.database.table)
        """
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        parts.append(table.name)
        return ".".join(parts)

    def _resolve_table_reference(self, ref: str, select_node: exp.Select) -> str:
        """
        Resolve a table reference (alias, CTE name, or actual table) to its canonical name.

        This works at any nesting level by only looking at the immediate SELECT context.
        For CTEs and subqueries, returns their alias name (which is the "table name" in that context).
        For actual tables with aliases, returns the actual table name.

        Args:
            ref: The table reference to resolve (could be alias, CTE name, or table name)
            select_node: The SELECT node containing the FROM/JOIN clauses

        Returns:
            The canonical table name (actual table for real tables, alias for CTEs/subqueries)
        """
        # Check if this is a CTE reference first
        # CTEs are defined in the WITH clause and referenced by their alias
        parent = select_node
        while parent:
            if isinstance(parent, (exp.Select, exp.Union)) and parent.args.get("with"):
                cte_node = parent.args["with"]
                for cte in cte_node.expressions:
                    if isinstance(cte, exp.CTE) and cte.alias == ref:
                        # This is a CTE - return the CTE alias as the "table name"
                        return ref
            parent = parent.parent if hasattr(parent, "parent") else None

        # Look for table references in FROM and JOIN clauses
        for table_ref in select_node.find_all(exp.Table):
            # Check if this table has the matching alias
            if table_ref.alias == ref:
                # Return the qualified table name
                parts = []
                if table_ref.db:
                    parts.append(table_ref.db)
                if table_ref.catalog:
                    parts.insert(0, table_ref.catalog)
                parts.append(table_ref.name)
                return ".".join(parts)
            # Also check if ref matches the table name directly (no alias case)
            elif table_ref.name == ref and not table_ref.alias:
                parts = []
                if table_ref.db:
                    parts.append(table_ref.db)
                if table_ref.catalog:
                    parts.insert(0, table_ref.catalog)
                parts.append(table_ref.name)
                return ".".join(parts)

        # Check for subquery aliases in FROM clause
        if select_node.args.get("from"):
            from_clause = select_node.args["from"]
            if isinstance(from_clause, exp.From):
                source = from_clause.this
                # Check if it's a subquery with matching alias
                if isinstance(source, exp.Subquery) and source.alias == ref:
                    # Return the subquery alias as the "table name"
                    return ref
                # Check if it's a table with matching alias
                elif isinstance(source, exp.Table) and source.alias == ref:
                    parts = []
                    if source.db:
                        parts.append(source.db)
                    if source.catalog:
                        parts.insert(0, source.catalog)
                    parts.append(source.name)
                    return ".".join(parts)

        # Check JOIN clauses for subqueries
        for join in select_node.find_all(exp.Join):
            if isinstance(join.this, exp.Subquery) and join.this.alias == ref:
                return ref
            elif isinstance(join.this, exp.Table) and join.this.alias == ref:
                parts = []
                if join.this.db:
                    parts.append(join.this.db)
                if join.this.catalog:
                    parts.insert(0, join.this.catalog)
                parts.append(join.this.name)
                return ".".join(parts)

        # If we can't resolve, return the reference as-is
        return ref

    def _infer_single_table_source(self, select_node: exp.Select) -> Optional[str]:
        """
        Infer the table name when there's only one table in FROM clause.

        This handles cases like "SELECT col FROM table" where col has no table prefix.

        Args:
            select_node: The SELECT node

        Returns:
            The table name if there's exactly one source, None otherwise
        """
        if not select_node.args.get("from"):
            return None

        from_clause = select_node.args["from"]
        if not isinstance(from_clause, exp.From):
            return None

        source = from_clause.this

        # Check for JOINs - if there are joins, we can't infer
        if list(select_node.find_all(exp.Join)):
            return None

        # Single table or CTE/subquery
        if isinstance(source, exp.Table):
            parts = []
            if source.db:
                parts.append(source.db)
            if source.catalog:
                parts.insert(0, source.catalog)
            if source.alias:
                # If the table has an alias, use the alias
                return source.alias
            parts.append(source.name)
            return ".".join(parts)
        elif isinstance(source, (exp.Subquery, exp.CTE)):
            # Return the subquery/CTE alias
            return source.alias if source.alias else None

        return None

    def _collect_source_columns(self, node: Node, sources: Set[str]) -> None:
        """
        Recursively collect all source columns from a lineage tree.

        This traverses the lineage tree depth-first, collecting leaf nodes
        which represent the actual source columns.

        Args:
            node: The current lineage node
            sources: Set to accumulate source column names
        """
        if not node.downstream:
            # Leaf node - this is a source column
            # Check if this is a literal value (SQLGlot uses position numbers for literals)
            if node.name.isdigit():
                # This is a literal - extract the actual value from the expression
                literal_repr = self._extract_literal_representation(node)
                sources.add(literal_repr)
            else:
                # SQLGlot's lineage provides qualified names, but may use aliases
                # Need to resolve aliases to actual table names
                qualified_name = self._resolve_source_column_alias(node.name)
                sources.add(qualified_name)
        else:
            # Traverse deeper into the tree
            for child in node.downstream:
                self._collect_source_columns(child, sources)

    def _extract_literal_representation(self, node: Node) -> str:
        """
        Extract a human-readable representation of a literal value from a lineage node.

        When SQLGlot encounters a literal value in a UNION branch, it returns the
        column position as the node name. This method extracts the actual literal
        value from the node's expression.

        Args:
            node: A lineage node where node.name is a digit (position number)

        Returns:
            A string like "<literal: NULL>" or "<literal: 'value'>" or "<literal: 0>"
        """
        try:
            expr = node.expression
            # The expression is typically an Alias wrapping the actual value
            if isinstance(expr, exp.Alias):
                literal_expr = expr.this
                literal_sql = literal_expr.sql(dialect=self.dialect)
                return f"<literal: {literal_sql}>"
            else:
                # Fallback: use the expression's SQL representation
                return f"<literal: {expr.sql(dialect=self.dialect)}>"
        except Exception:
            # If extraction fails, return a generic literal marker
            return "<literal>"

    def _get_query_tables(self) -> List[str]:
        """
        Get all table names referenced in the current query.

        Returns:
            List of fully qualified table names used in the query
        """
        tables = []
        for table_node in self.expr.find_all(exp.Table):
            table_name = self._get_qualified_table_name(table_node)
            tables.append(table_name)
        return tables

    def _resolve_source_column_alias(self, column_name: str) -> str:
        """
        Resolve table aliases in source column names.

        This searches through ALL SELECT nodes in the query (including nested ones)
        to find and resolve table aliases, CTEs, and subqueries.

        Args:
            column_name: Column name like "alias.column" or "table.column"

        Returns:
            Fully qualified column name with actual table name
        """
        # Parse the column name (format: table.column or db.table.column)
        parts = column_name.split(".")

        if len(parts) < 2:
            # No table qualifier, return as-is
            return column_name

        # The table part might be an alias, CTE name, or actual table
        table_part = parts[0] if len(parts) == 2 else parts[-2]
        column_part = parts[-1]

        # Try to resolve by searching through ALL SELECT nodes (including nested)
        # This handles cases where the alias is defined deep in a subquery/CTE
        for select_node in self.expr.find_all(exp.Select):
            resolved = self._resolve_table_reference(table_part, select_node)
            # If resolution changed the name, we found it
            if resolved != table_part:
                # Reconstruct with resolved table name
                if len(parts) == 2:
                    return f"{resolved}.{column_part}"
                else:
                    # Has database part
                    return f"{parts[0]}.{resolved}.{column_part}"

        # If we couldn't resolve in any SELECT, return as-is
        return column_name

    def _generate_query_preview(self, expr: exp.Expression) -> str:
        """
        Generate a preview string for a query (first 100 chars, normalized).

        Args:
            expr: The SQL expression to generate a preview for

        Returns:
            Preview string (first 100 chars with "..." if truncated)
        """
        query_text = expr.sql(dialect=self.dialect)
        preview = " ".join(query_text.split())[:100]
        if len(" ".join(query_text.split())) > 100:
            preview += "..."
        return preview

    def _filter_by_table(self, expr: exp.Expression, table_filter: str) -> bool:
        """
        Check if a query references a specific table.

        Args:
            expr: The SQL expression to check
            table_filter: Table name to filter by (case-insensitive partial match)

        Returns:
            True if the query references the table, False otherwise
        """
        # Temporarily swap self.expr to analyze this expression
        original_expr = self.expr
        self.expr = expr
        try:
            query_tables = self._get_query_tables()
            table_filter_lower = table_filter.lower()
            return any(table_filter_lower in table.lower() for table in query_tables)
        finally:
            self.expr = original_expr

    def _iterate_queries(
        self, table_filter: Optional[str] = None
    ) -> Iterator[Tuple[int, exp.Expression, str]]:
        """
        Iterate over queries with filtering and preview generation.

        Args:
            table_filter: Optional table name to filter queries by

        Yields:
            Tuple of (query_index, expression, query_preview)
        """
        for idx, expr in enumerate(self.expressions):
            # Apply table filter
            if table_filter and not self._filter_by_table(expr, table_filter):
                continue

            # Generate preview
            preview = self._generate_query_preview(expr)

            yield idx, expr, preview
