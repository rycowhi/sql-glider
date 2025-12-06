"""Core lineage analysis using SQLGlot."""

from typing import List, Optional, Set

from pydantic import BaseModel, Field
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from sqlglot.lineage import Node, lineage


class ColumnLineage(BaseModel):
    """Represents lineage for a single output column."""

    output_column: str = Field(..., description="The output column name")
    source_columns: List[str] = Field(
        default_factory=list,
        description="Fully qualified source columns (e.g., table.column)",
    )


class TableLineage(BaseModel):
    """Represents lineage for tables."""

    output_table: str = Field(..., description="The output table name")
    source_tables: List[str] = Field(
        default_factory=list, description="Source table names"
    )


class LineageAnalyzer:
    """Analyze column and table lineage for SQL queries."""

    def __init__(self, sql: str, dialect: str = "spark"):
        """
        Initialize the lineage analyzer.

        Args:
            sql: SQL query string to analyze
            dialect: SQL dialect (default: spark)

        Raises:
            ParseError: If the SQL cannot be parsed
        """
        self.sql = sql
        self.dialect = dialect

        try:
            self.expr = parse_one(sql, dialect=dialect)
        except ParseError as e:
            raise ParseError(f"Invalid SQL syntax: {e}") from e

    def get_output_columns(self) -> List[str]:
        """
        Extract all output column names from the query with full qualification.

        For DML/DDL statements (INSERT, UPDATE, MERGE, CREATE TABLE AS, etc.),
        returns the target table columns. For DQL (SELECT), returns the selected columns.

        Returns:
            List of fully qualified output column names (table.column or database.table.column)
        """
        columns = []

        # Build mapping for qualified names
        self._column_mapping = {}  # Maps qualified name -> lineage column name

        # Check if this is a DML/DDL statement
        target_table, select_node = self._get_target_and_select()

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

    def analyze_column_lineage(
        self, column: Optional[str] = None
    ) -> List[ColumnLineage]:
        """
        Analyze column lineage for the query.

        Args:
            column: Optional specific column to analyze. If None, analyzes all columns.
                    Column matching is case-insensitive.

        Returns:
            List of ColumnLineage objects representing the lineage for each column

        Raises:
            ValueError: If the specified column is not found in the query output
        """
        output_columns = self.get_output_columns()

        if column:
            # Analyze only the specified column (case-insensitive matching)
            # Find the actual column name that matches (preserving original case)
            matched_column = None
            column_lower = column.lower()
            for output_col in output_columns:
                if output_col.lower() == column_lower:
                    matched_column = output_col
                    break

            if matched_column is None:
                raise ValueError(
                    f"Column '{column}' not found in query output. "
                    f"Available columns: {', '.join(output_columns)}"
                )
            columns_to_analyze = [matched_column]
        else:
            # Analyze all columns
            columns_to_analyze = output_columns

        results = []
        for col in columns_to_analyze:
            try:
                # Get the column name that lineage expects (may be different from qualified name)
                lineage_col = self._column_mapping.get(col, col)

                # Get lineage tree for this column
                node = lineage(lineage_col, self.sql, dialect=self.dialect)

                # Collect all source columns
                sources: Set[str] = set()
                self._collect_source_columns(node, sources)

                results.append(
                    ColumnLineage(output_column=col, source_columns=sorted(sources))
                )
            except Exception:
                # If lineage fails for a column, return empty sources
                # This can happen for literal values or complex expressions
                results.append(ColumnLineage(output_column=col, source_columns=[]))

        return results

    def analyze_table_lineage(self) -> TableLineage:
        """
        Analyze table-level lineage for the query.

        Returns:
            TableLineage object representing which tables are used

        Note:
            This is a simplified implementation that finds all table references.
            For more complex queries (CTEs, subqueries), this may need enhancement.
        """
        source_tables: Set[str] = set()

        # Find all Table nodes in the AST
        for table_node in self.expr.find_all(exp.Table):
            # Get fully qualified table name
            table_name = table_node.sql(dialect=self.dialect)
            source_tables.add(table_name)

        # The output table would typically be defined in INSERT/CREATE statements
        # For SELECT statements, we don't have an explicit output table
        output_table = "query_result"

        return TableLineage(
            output_table=output_table, source_tables=sorted(source_tables)
        )

    def analyze_reverse_lineage(self, source_column: str) -> List[ColumnLineage]:
        """
        Analyze reverse lineage (impact analysis) for a source column.

        This method determines which output columns are affected by a given source column.
        It works by running forward lineage analysis on all output columns, then inverting
        the dependency graph to find outputs that depend on the specified source.

        Note: This method reuses the ColumnLineage model with semantic field swap:
        - output_column: The source column being analyzed (what you're looking at)
        - source_columns: The affected output columns (what gets impacted)

        Args:
            source_column: Fully qualified source column to analyze (e.g., "table.column").
                          Column matching is case-insensitive.

        Returns:
            List containing one ColumnLineage object where:
            - output_column = the source column being analyzed
            - source_columns = list of affected output columns

        Raises:
            ValueError: If source_column is not found in the query sources

        Example:
            >>> analyzer = LineageAnalyzer("SELECT customer_id FROM orders")
            >>> result = analyzer.analyze_reverse_lineage("orders.customer_id")
            >>> print(result[0].output_column)  # "orders.customer_id"
            >>> print(result[0].source_columns)  # ["customer_id"]
        """
        # Step 1: Run forward lineage on all output columns
        forward_results = self.analyze_column_lineage(column=None)

        # Step 2: Build reverse mapping (source -> [affected outputs])
        reverse_map: dict[str, list[str]] = {}
        for result in forward_results:
            for source in result.source_columns:
                if source not in reverse_map:
                    reverse_map[source] = []
                reverse_map[source].append(result.output_column)

        # Step 3: Validate source exists (case-insensitive matching)
        matched_source = None
        source_column_lower = source_column.lower()
        for source in reverse_map.keys():
            if source.lower() == source_column_lower:
                matched_source = source
                break

        if matched_source is None:
            available = sorted(reverse_map.keys())
            raise ValueError(
                f"Source column '{source_column}' not found in query sources. "
                f"Available source columns: {', '.join(available)}"
            )

        # Step 4: Return with semantic swap (source as output, affected as sources)
        return [
            ColumnLineage(
                output_column=matched_source,
                source_columns=sorted(reverse_map[matched_source]),
            )
        ]

    def _get_target_and_select(self) -> tuple[Optional[str], exp.Select]:
        """
        Detect if this is a DML/DDL statement and extract the target table and SELECT node.

        Returns:
            Tuple of (target_table_name, select_node) where:
            - target_table_name is the fully qualified target table for DML/DDL, or None for pure SELECT
            - select_node is the SELECT statement that provides the data

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

        # Check for CREATE TABLE AS SELECT (CTAS)
        elif isinstance(self.expr, exp.Create):
            if self.expr.kind == "TABLE":
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

        # No SELECT found
        raise ValueError("No SELECT statement found in the query")

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
            # SQLGlot's lineage provides qualified names, but may use aliases
            # Need to resolve aliases to actual table names
            qualified_name = self._resolve_source_column_alias(node.name)
            sources.add(qualified_name)
        else:
            # Traverse deeper into the tree
            for child in node.downstream:
                self._collect_source_columns(child, sources)

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
