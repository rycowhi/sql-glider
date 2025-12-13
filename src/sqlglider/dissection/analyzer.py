"""SQL query dissection analyzer."""

from typing import List, Optional, Set, Tuple

from sqlglot import exp, parse
from sqlglot.errors import ParseError

from sqlglider.dissection.models import (
    ComponentType,
    QueryDissectionResult,
    QueryMetadata,
    SQLComponent,
)


class DissectionAnalyzer:
    """Analyze and dissect SQL queries into components."""

    def __init__(self, sql: str, dialect: str = "spark"):
        """
        Initialize the dissection analyzer.

        Args:
            sql: SQL query string (can contain multiple statements)
            dialect: SQL dialect (default: spark)

        Raises:
            ParseError: If the SQL cannot be parsed
        """
        self.sql = sql
        self.dialect = dialect

        try:
            # Parse all statements
            parsed = parse(sql, dialect=dialect)

            # Filter out None values (can happen with empty statements or comments)
            self.expressions: List[exp.Expression] = [
                expr for expr in parsed if expr is not None
            ]

            if not self.expressions:
                raise ParseError("No valid SQL statements found")

        except ParseError as e:
            raise ParseError(f"Invalid SQL syntax: {e}") from e

    def dissect_queries(self) -> List[QueryDissectionResult]:
        """
        Dissect all queries in the SQL file.

        Returns:
            List of QueryDissectionResult objects (one per query)
        """
        results = []

        for query_index, expr in enumerate(self.expressions):
            result = self._dissect_single_query(expr, query_index)
            results.append(result)

        return results

    def _dissect_single_query(
        self, expr: exp.Expression, query_index: int
    ) -> QueryDissectionResult:
        """
        Dissect a single query into components.

        Args:
            expr: SQLGlot expression to dissect
            query_index: Index of query in multi-query file

        Returns:
            QueryDissectionResult with all extracted components
        """
        components: List[SQLComponent] = []
        component_counter = 0

        # Get statement type
        stmt_type = self._get_statement_type(expr)

        # Track CTE names for dependency analysis
        cte_names: Set[str] = set()

        # Extract CTEs (if present)
        if hasattr(expr, "args") and expr.args.get("with"):
            with_clause = expr.args["with"]
            for cte in with_clause.expressions:
                if isinstance(cte, exp.CTE) and cte.alias:
                    cte_name = cte.alias
                    cte_names.add(cte_name)
                    cte_sql = cte.this.sql(dialect=self.dialect)

                    # Extract dependencies (references to other CTEs)
                    cte_deps = self._extract_cte_dependencies(cte.this, cte_names)

                    components.append(
                        SQLComponent(
                            component_type=ComponentType.CTE,
                            component_index=component_counter,
                            name=cte_name,
                            sql=cte_sql,
                            parent_index=None,
                            depth=0,
                            is_executable=True,
                            dependencies=cte_deps,
                            location="WITH clause",
                        )
                    )

                    # Extract scalar subqueries within CTE
                    component_counter += 1
                    component_counter = self._extract_scalar_subqueries(
                        cte.this,
                        components,
                        component_counter,
                        cte_names,
                        parent_index=component_counter - 1,
                        depth=1,
                        parent_context=f"CTE '{cte_name}'",
                    )

        # Extract target table (for DML/DDL)
        target_table, target_type = self._get_target_table(expr)
        if target_table:
            components.append(
                SQLComponent(
                    component_type=ComponentType.TARGET_TABLE,
                    component_index=component_counter,
                    name=target_table,
                    sql=target_table,
                    parent_index=None,
                    depth=0,
                    is_executable=False,
                    dependencies=[],
                    location=f"{target_type} target",
                )
            )
            component_counter += 1

        # Extract source query (for INSERT/CTAS/MERGE)
        source_select = self._get_source_select(expr)
        if source_select:
            source_sql = source_select.sql(dialect=self.dialect)
            source_deps = self._extract_cte_dependencies(source_select, cte_names)
            source_index = component_counter

            components.append(
                SQLComponent(
                    component_type=ComponentType.SOURCE_QUERY,
                    component_index=component_counter,
                    name=None,
                    sql=source_sql,
                    parent_index=None,
                    depth=0,
                    is_executable=True,
                    dependencies=source_deps,
                    location=self._get_source_location(expr),
                )
            )
            component_counter += 1

            # Check if source is a UNION and extract branches
            if self._is_union(source_select):
                component_counter = self._extract_union_branches(
                    source_select,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=source_index,
                    depth=1,
                )
            else:
                # Extract subqueries from source SELECT
                component_counter = self._extract_subqueries(
                    source_select,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=source_index,
                    depth=1,
                    parent_context="SOURCE_QUERY",
                )

                # Extract scalar subqueries from source SELECT
                component_counter = self._extract_scalar_subqueries(
                    source_select,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=source_index,
                    depth=1,
                    parent_context="SOURCE_QUERY",
                )

        # Handle pure SELECT (no DML/DDL wrapper)
        elif isinstance(expr, (exp.Select, exp.Union)):
            main_sql = expr.sql(dialect=self.dialect)
            main_deps = self._extract_cte_dependencies(expr, cte_names)
            main_index = component_counter

            components.append(
                SQLComponent(
                    component_type=ComponentType.MAIN_QUERY,
                    component_index=component_counter,
                    name=None,
                    sql=main_sql,
                    parent_index=None,
                    depth=0,
                    is_executable=True,
                    dependencies=main_deps,
                    location="Top-level query",
                )
            )
            component_counter += 1

            # Check if main query is a UNION and extract branches
            if self._is_union(expr):
                component_counter = self._extract_union_branches(
                    expr,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=main_index,
                    depth=1,
                )
            else:
                # Extract FROM-clause subqueries from main SELECT
                component_counter = self._extract_subqueries(
                    expr,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=main_index,
                    depth=1,
                    parent_context="MAIN_QUERY",
                )

                # Extract scalar subqueries from main SELECT
                component_counter = self._extract_scalar_subqueries(
                    expr,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=main_index,
                    depth=1,
                    parent_context="MAIN_QUERY",
                )

        # Create metadata
        preview = self._generate_query_preview(expr)
        metadata = QueryMetadata(
            query_index=query_index,
            query_preview=preview,
            statement_type=stmt_type,
            total_components=len(components),
        )

        # Get original SQL for this query
        original_sql = expr.sql(dialect=self.dialect)

        return QueryDissectionResult(
            metadata=metadata,
            components=components,
            original_sql=original_sql,
        )

    def _is_union(self, node: exp.Expression) -> bool:
        """Check if an expression is a UNION/UNION ALL."""
        return isinstance(node, exp.Union)

    def _extract_union_branches(
        self,
        union_node: exp.Expression,
        components: List[SQLComponent],
        component_counter: int,
        cte_names: Set[str],
        parent_index: int,
        depth: int,
    ) -> int:
        """
        Extract individual branches from a UNION/UNION ALL.

        Args:
            union_node: The UNION expression
            components: List to append components to
            component_counter: Current component index counter
            cte_names: Set of CTE names for dependency analysis
            parent_index: Index of parent component
            depth: Current nesting depth

        Returns:
            Updated component_counter
        """
        branches = self._flatten_union(union_node)
        total_branches = len(branches)

        for i, (branch, union_type) in enumerate(branches):
            branch_sql = branch.sql(dialect=self.dialect)
            branch_deps = self._extract_cte_dependencies(branch, cte_names)

            # Determine location string
            if union_type:
                location = f"UNION branch {i + 1} of {total_branches} ({union_type})"
            else:
                location = f"UNION branch {i + 1} of {total_branches}"

            branch_index = component_counter
            components.append(
                SQLComponent(
                    component_type=ComponentType.UNION_BRANCH,
                    component_index=component_counter,
                    name=f"branch_{i}",
                    sql=branch_sql,
                    parent_index=parent_index,
                    depth=depth,
                    is_executable=True,
                    dependencies=branch_deps,
                    location=location,
                )
            )
            component_counter += 1

            # Extract subqueries and scalar subqueries from each branch
            component_counter = self._extract_subqueries(
                branch,
                components,
                component_counter,
                cte_names,
                parent_index=branch_index,
                depth=depth + 1,
                parent_context=f"UNION_BRANCH '{f'branch_{i}'}'",
            )

            component_counter = self._extract_scalar_subqueries(
                branch,
                components,
                component_counter,
                cte_names,
                parent_index=branch_index,
                depth=depth + 1,
                parent_context=f"UNION_BRANCH '{f'branch_{i}'}'",
            )

        return component_counter

    def _flatten_union(
        self, node: exp.Expression
    ) -> List[Tuple[exp.Expression, Optional[str]]]:
        """
        Flatten a UNION tree into a list of (branch, union_type) tuples.

        Args:
            node: The UNION expression to flatten

        Returns:
            List of (SELECT expression, union_type) tuples where union_type
            is 'UNION', 'UNION ALL', or None for the first branch
        """
        branches: List[Tuple[exp.Expression, Optional[str]]] = []

        def collect_branches(
            n: exp.Expression,
            is_right: bool = False,
            parent_union_type: Optional[str] = None,
        ) -> None:
            if isinstance(n, exp.Union):
                # Determine union type (UNION vs UNION ALL)
                union_type = "UNION ALL" if n.args.get("distinct") is False else "UNION"

                # Process left branch first
                collect_branches(n.this, is_right=False, parent_union_type=None)

                # Process right branch with the union type
                collect_branches(
                    n.expression, is_right=True, parent_union_type=union_type
                )
            else:
                # Leaf SELECT node
                branches.append((n, parent_union_type))

        collect_branches(node)
        return branches

    def _extract_subqueries(
        self,
        node: exp.Expression,
        components: List[SQLComponent],
        component_counter: int,
        cte_names: Set[str],
        parent_index: int,
        depth: int,
        parent_context: str,
    ) -> int:
        """
        Extract FROM-clause subqueries from a SELECT node.

        Args:
            node: SELECT expression to search
            components: List to append components to
            component_counter: Current component index counter
            cte_names: Set of CTE names for dependency analysis
            parent_index: Index of parent component
            depth: Current nesting depth
            parent_context: Description of parent for location string

        Returns:
            Updated component_counter
        """
        # Find direct child subqueries in FROM clause
        for subquery in node.find_all(exp.Subquery):
            # Only process direct subqueries in FROM clause
            # Skip scalar subqueries (handled separately)
            parent = subquery.parent
            if parent is None:
                continue

            # Check if this is a FROM-clause subquery (in From or Join)
            is_from_subquery = False
            current = subquery
            while current.parent:
                if isinstance(current.parent, (exp.From, exp.Join)):
                    is_from_subquery = True
                    break
                if isinstance(current.parent, (exp.Select, exp.Where, exp.Having)):
                    break
                current = current.parent

            if not is_from_subquery:
                continue

            # Skip if already processed at a shallower level
            if self._is_nested_in_already_extracted(subquery, components):
                continue

            subquery_sql = subquery.this.sql(dialect=self.dialect)
            subquery_alias = subquery.alias or f"subquery_{component_counter}"
            subquery_deps = self._extract_cte_dependencies(subquery.this, cte_names)

            current_index = component_counter
            components.append(
                SQLComponent(
                    component_type=ComponentType.SUBQUERY,
                    component_index=component_counter,
                    name=subquery_alias,
                    sql=subquery_sql,
                    parent_index=parent_index,
                    depth=depth,
                    is_executable=True,
                    dependencies=subquery_deps,
                    location=f"FROM clause in {parent_context}",
                )
            )
            component_counter += 1

            # Recursively extract nested subqueries
            if isinstance(subquery.this, exp.Select):
                component_counter = self._extract_subqueries(
                    subquery.this,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=current_index,
                    depth=depth + 1,
                    parent_context=f"SUBQUERY '{subquery_alias}'",
                )

                component_counter = self._extract_scalar_subqueries(
                    subquery.this,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=current_index,
                    depth=depth + 1,
                    parent_context=f"SUBQUERY '{subquery_alias}'",
                )

        return component_counter

    def _extract_scalar_subqueries(
        self,
        node: exp.Expression,
        components: List[SQLComponent],
        component_counter: int,
        cte_names: Set[str],
        parent_index: int,
        depth: int,
        parent_context: str,
    ) -> int:
        """
        Extract scalar subqueries from SELECT list, WHERE, HAVING clauses.

        Args:
            node: Expression to search for scalar subqueries
            components: List to append components to
            component_counter: Current component index counter
            cte_names: Set of CTE names for dependency analysis
            parent_index: Index of parent component
            depth: Current nesting depth
            parent_context: Description of parent for location string

        Returns:
            Updated component_counter
        """
        # Look for subqueries that are NOT in FROM clause
        for subquery in node.find_all(exp.Subquery):
            # Skip if already processed at a shallower level
            if self._is_nested_in_already_extracted(subquery, components):
                continue

            # Determine location context
            location_context = self._get_scalar_subquery_location(
                subquery, parent_context
            )
            if location_context is None:
                # This is a FROM-clause subquery, skip
                continue

            subquery_sql = subquery.this.sql(dialect=self.dialect)
            # For scalar subqueries, try to get the alias from the parent column
            subquery_name = self._get_scalar_subquery_name(subquery)
            subquery_deps = self._extract_cte_dependencies(subquery.this, cte_names)

            current_index = component_counter
            components.append(
                SQLComponent(
                    component_type=ComponentType.SCALAR_SUBQUERY,
                    component_index=component_counter,
                    name=subquery_name,
                    sql=subquery_sql,
                    parent_index=parent_index,
                    depth=depth,
                    is_executable=True,
                    dependencies=subquery_deps,
                    location=location_context,
                )
            )
            component_counter += 1

            # Recursively extract nested subqueries
            if isinstance(subquery.this, exp.Select):
                component_counter = self._extract_scalar_subqueries(
                    subquery.this,
                    components,
                    component_counter,
                    cte_names,
                    parent_index=current_index,
                    depth=depth + 1,
                    parent_context="SCALAR_SUBQUERY",
                )

        return component_counter

    def _get_scalar_subquery_location(
        self, subquery: exp.Subquery, parent_context: str
    ) -> Optional[str]:
        """
        Determine the location of a scalar subquery.

        Returns None if this is a FROM-clause subquery.
        """
        current = subquery
        while current.parent:
            parent = current.parent

            # If we hit FROM or JOIN before SELECT/WHERE/HAVING, it's not scalar
            if isinstance(parent, (exp.From, exp.Join)):
                return None

            # Found in SELECT list
            if isinstance(parent, exp.Select):
                # Check if subquery is in the expressions (SELECT list)
                if current in getattr(parent, "expressions", []):
                    return f"SELECT list in {parent_context}"

            # Found in WHERE clause
            if isinstance(parent, exp.Where):
                return f"WHERE clause in {parent_context}"

            # Found in HAVING clause
            if isinstance(parent, exp.Having):
                return f"HAVING clause in {parent_context}"

            # Found in comparison or other expression
            if isinstance(
                parent,
                (
                    exp.EQ,
                    exp.GT,
                    exp.GTE,
                    exp.LT,
                    exp.LTE,
                    exp.NEQ,
                    exp.In,
                    exp.Between,
                ),
            ):
                # Continue up to find WHERE/HAVING/SELECT
                pass

            current = parent

        # Default - assume it's in the query somewhere
        return f"Expression in {parent_context}"

    def _get_scalar_subquery_name(self, subquery: exp.Subquery) -> Optional[str]:
        """Get the alias/name for a scalar subquery if available."""
        # Check if subquery has a direct alias
        if subquery.alias:
            return subquery.alias

        # Check if parent is a column alias
        if subquery.parent and isinstance(subquery.parent, exp.Alias):
            return subquery.parent.alias

        return None

    def _is_nested_in_already_extracted(
        self, subquery: exp.Subquery, components: List[SQLComponent]
    ) -> bool:
        """Check if this subquery is nested inside an already-extracted subquery."""
        # Get the SQL of this subquery
        subquery_sql = subquery.this.sql(dialect=self.dialect)

        # Check if any existing component's SQL contains this subquery's SQL
        # (but is not exactly equal to it)
        for comp in components:
            if comp.component_type in (
                ComponentType.SUBQUERY,
                ComponentType.SCALAR_SUBQUERY,
            ):
                if subquery_sql in comp.sql and subquery_sql != comp.sql:
                    return True

        return False

    def _extract_cte_dependencies(
        self, node: exp.Expression, cte_names: Set[str]
    ) -> List[str]:
        """
        Extract CTE dependencies from an expression.

        Args:
            node: SQLGlot expression to analyze
            cte_names: Set of CTE names defined in the query

        Returns:
            Sorted list of CTE names this expression depends on
        """
        dependencies: Set[str] = set()

        # Find all table references
        for table_node in node.find_all(exp.Table):
            table_name = table_node.name
            if table_name in cte_names:
                dependencies.add(table_name)

        return sorted(dependencies)

    def _get_target_table(
        self, expr: exp.Expression
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Get target table name and statement type for DML/DDL.

        Returns:
            Tuple of (table_name, location_prefix) or (None, None)
        """
        if isinstance(expr, exp.Insert):
            target = expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), "INSERT INTO")

        elif isinstance(expr, exp.Create):
            kind = getattr(expr, "kind", "").upper()
            target = expr.this
            if isinstance(target, exp.Schema):
                target = target.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), f"CREATE {kind}")

        elif isinstance(expr, exp.Merge):
            target = expr.this
            if isinstance(target, exp.Table):
                return (self._get_qualified_table_name(target), "MERGE INTO")

        return (None, None)

    def _get_source_select(self, expr: exp.Expression) -> Optional[exp.Expression]:
        """
        Get the source SELECT/UNION from INSERT/CTAS/MERGE.

        Returns:
            SELECT/UNION expression or None
        """
        if isinstance(expr, exp.Insert):
            source = expr.expression
            if isinstance(source, (exp.Select, exp.Union)):
                return source

        elif isinstance(expr, exp.Create):
            source = expr.expression
            if isinstance(source, (exp.Select, exp.Union)):
                return source

        elif isinstance(expr, exp.Merge):
            # For MERGE, find the USING clause's SELECT/UNION
            using = expr.args.get("using")
            if using:
                if isinstance(using, (exp.Select, exp.Union)):
                    return using
                elif isinstance(using, exp.Subquery):
                    return using.this
            # Fallback: find all SELECTs
            select_nodes = list(expr.find_all(exp.Select))
            if select_nodes:
                return select_nodes[0]

        return None

    def _get_source_location(self, expr: exp.Expression) -> str:
        """Get the location description for source query."""
        if isinstance(expr, exp.Insert):
            return "INSERT source SELECT"
        elif isinstance(expr, exp.Create):
            kind = getattr(expr, "kind", "").upper()
            return f"CREATE {kind} AS SELECT"
        elif isinstance(expr, exp.Merge):
            return "MERGE USING clause"
        return "Source SELECT"

    def _get_qualified_table_name(self, table: exp.Table) -> str:
        """Get fully qualified table name."""
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        parts.append(table.name)
        return ".".join(parts)

    def _get_statement_type(self, expr: exp.Expression) -> str:
        """Get human-readable statement type."""
        expr_type = type(expr).__name__

        if isinstance(expr, exp.Create):
            kind = getattr(expr, "kind", "")
            return f"CREATE {kind}".strip()

        type_map = {
            "Select": "SELECT",
            "Insert": "INSERT",
            "Merge": "MERGE",
            "Update": "UPDATE",
            "Delete": "DELETE",
            "Union": "SELECT",  # UNION is still a SELECT-type query
        }

        return type_map.get(expr_type, expr_type.upper())

    def _generate_query_preview(self, expr: exp.Expression) -> str:
        """Generate preview string (first 100 chars)."""
        query_text = expr.sql(dialect=self.dialect)
        preview = " ".join(query_text.split())[:100]
        if len(" ".join(query_text.split())) > 100:
            preview += "..."
        return preview
