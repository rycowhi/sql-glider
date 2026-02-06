"""Graph query functionality for upstream/downstream analysis."""

from pathlib import Path
from typing import Dict, List, Optional

import rustworkx as rx

from sqlglider.graph.models import GraphNode, LineageGraph, LineageNode, LineagePath
from sqlglider.graph.serialization import load_graph, to_rustworkx


class LineageQueryResult:
    """Result of a lineage query."""

    def __init__(
        self,
        query_column: str,
        direction: str,  # "upstream" or "downstream"
        related_columns: List[LineageNode],
        queried_columns: Optional[List[str]] = None,
        is_table_query: bool = False,
    ):
        """
        Initialize query result.

        Args:
            query_column: The column or table that was queried
            direction: Query direction ("upstream" or "downstream")
            related_columns: List of related LineageNode objects with hop info
            queried_columns: List of column identifiers that were queried
                (for table-level queries, contains all columns in the table)
            is_table_query: True if this is a table-level query
        """
        self.query_column = query_column
        self.direction = direction
        self.related_columns = related_columns
        self.queried_columns = queried_columns or [query_column]
        self.is_table_query = is_table_query

    def __len__(self) -> int:
        """Return number of related columns."""
        return len(self.related_columns)

    def __iter__(self):
        """Iterate over related columns."""
        return iter(self.related_columns)


class GraphQuerier:
    """Query lineage graphs for upstream/downstream dependencies."""

    def __init__(self, graph: LineageGraph):
        """
        Initialize the querier with a graph.

        Args:
            graph: LineageGraph to query
        """
        self.graph = graph
        self.rx_graph, self.node_map = to_rustworkx(graph)
        self._reverse_map = {v: k for k, v in self.node_map.items()}
        # Create reversed graph for upstream queries (lazy initialization)
        self._rx_graph_reversed: Optional[rx.PyDiGraph] = None

    @property
    def rx_graph_reversed(self) -> rx.PyDiGraph:
        """Get reversed graph for upstream traversal (created lazily)."""
        if self._rx_graph_reversed is None:
            self._rx_graph_reversed = self.rx_graph.copy()
            self._rx_graph_reversed.reverse()
        return self._rx_graph_reversed

    def _is_root(self, node_idx: int) -> bool:
        """Check if node is a root (no incoming edges in original graph)."""
        return self.rx_graph.in_degree(node_idx) == 0

    def _is_leaf(self, node_idx: int) -> bool:
        """Check if node is a leaf (no outgoing edges in original graph)."""
        return self.rx_graph.out_degree(node_idx) == 0

    def _find_all_paths(
        self,
        from_idx: int,
        to_idx: int,
        use_reversed: bool = False,
    ) -> List[List[int]]:
        """
        Find all simple paths between two nodes.

        Args:
            from_idx: Starting node index
            to_idx: Target node index
            use_reversed: If True, use reversed graph for upstream queries

        Returns:
            List of paths, where each path is a list of node indices
        """
        graph = self.rx_graph_reversed if use_reversed else self.rx_graph
        return rx.all_simple_paths(graph, from_idx, to_idx)

    def _convert_path_to_identifiers(
        self,
        path: List[int],
        reverse: bool = False,
    ) -> LineagePath:
        """
        Convert a path of node indices to a LineagePath with identifiers.

        Args:
            path: List of node indices
            reverse: If True, reverse the path order (for upstream queries)

        Returns:
            LineagePath with node identifiers
        """
        identifiers = [self._reverse_map[idx] for idx in path]
        if reverse:
            identifiers = list(reversed(identifiers))
        return LineagePath(nodes=identifiers)

    @classmethod
    def from_file(cls, graph_path: Path) -> "GraphQuerier":
        """
        Create a querier from a graph file.

        Args:
            graph_path: Path to graph JSON file

        Returns:
            GraphQuerier instance

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        graph = load_graph(graph_path)
        return cls(graph)

    def find_upstream(self, column: str) -> LineageQueryResult:
        """
        Find all upstream (source) columns for a given column.

        Uses dijkstra_shortest_path_lengths on a reversed graph to find all
        nodes that have a path leading to the specified column, with hop counts,
        root/leaf detection, and full path information.

        Args:
            column: Column identifier to analyze

        Returns:
            LineageQueryResult with upstream columns including:
            - hop distances (shortest path)
            - is_root/is_leaf flags
            - all paths to the queried column

        Raises:
            ValueError: If column not found in graph
        """
        # Case-insensitive lookup
        matched_column = self._find_column(column)
        if matched_column is None:
            raise ValueError(f"Column '{column}' not found in graph")

        target_idx = self.node_map[matched_column]

        # Use dijkstra on reversed graph to get distances to all ancestors
        # Each edge has weight 1.0 for hop counting
        distances = rx.dijkstra_shortest_path_lengths(
            self.rx_graph_reversed,
            target_idx,
            edge_cost_fn=lambda _: 1.0,
        )

        # Build LineageNode for each reachable node
        upstream_columns = []
        for idx, hops in distances.items():
            node_data = self.rx_graph[idx]

            # Find all paths from this node to target
            # On reversed graph: from target to this node, then reverse the paths
            raw_paths = self._find_all_paths(target_idx, idx, use_reversed=True)
            paths = [
                self._convert_path_to_identifiers(p, reverse=True) for p in raw_paths
            ]

            upstream_columns.append(
                LineageNode.from_graph_node(
                    GraphNode(**node_data),
                    hops=int(hops),
                    output_column=matched_column,
                    is_root=self._is_root(idx),
                    is_leaf=self._is_leaf(idx),
                    paths=paths,
                )
            )

        # Sort by identifier for consistent output
        upstream_columns.sort(key=lambda n: n.identifier.lower())

        return LineageQueryResult(
            query_column=matched_column,
            direction="upstream",
            related_columns=upstream_columns,
        )

    def find_downstream(self, column: str) -> LineageQueryResult:
        """
        Find all downstream (affected) columns for a given column.

        Uses dijkstra_shortest_path_lengths to find all nodes that have a path
        from the specified column, with hop counts, root/leaf detection, and
        full path information.

        Args:
            column: Column identifier to analyze

        Returns:
            LineageQueryResult with downstream columns including:
            - hop distances (shortest path)
            - is_root/is_leaf flags
            - all paths from the queried column

        Raises:
            ValueError: If column not found in graph
        """
        # Case-insensitive lookup
        matched_column = self._find_column(column)
        if matched_column is None:
            raise ValueError(f"Column '{column}' not found in graph")

        source_idx = self.node_map[matched_column]

        # Use dijkstra on original graph to get distances to all descendants
        # Each edge has weight 1.0 for hop counting
        distances = rx.dijkstra_shortest_path_lengths(
            self.rx_graph,
            source_idx,
            edge_cost_fn=lambda _: 1.0,
        )

        # Build LineageNode for each reachable node
        downstream_columns = []
        for idx, hops in distances.items():
            node_data = self.rx_graph[idx]

            # Find all paths from source to this node
            raw_paths = self._find_all_paths(source_idx, idx, use_reversed=False)
            paths = [
                self._convert_path_to_identifiers(p, reverse=False) for p in raw_paths
            ]

            downstream_columns.append(
                LineageNode.from_graph_node(
                    GraphNode(**node_data),
                    hops=int(hops),
                    output_column=matched_column,
                    is_root=self._is_root(idx),
                    is_leaf=self._is_leaf(idx),
                    paths=paths,
                )
            )

        # Sort by identifier for consistent output
        downstream_columns.sort(key=lambda n: n.identifier.lower())

        return LineageQueryResult(
            query_column=matched_column,
            direction="downstream",
            related_columns=downstream_columns,
        )

    def _find_column(self, column: str) -> Optional[str]:
        """
        Find column with case-insensitive matching.

        Args:
            column: Column identifier to find

        Returns:
            Matched column identifier or None
        """
        column_lower = column.lower()
        for identifier in self.node_map.keys():
            if identifier.lower() == column_lower:
                return identifier
        return None

    def _find_table_columns(self, table: str) -> List[str]:
        """
        Find all column identifiers belonging to a table.

        Supports two matching modes:
        - Single part (e.g., "orders"): Matches any column where GraphNode.table == "orders"
          (cross-schema matching - matches prod.orders.col AND staging.orders.col)
        - Two parts (e.g., "prod.orders"): Matches columns where identifier starts with
          "prod.orders." (schema-qualified matching)

        Args:
            table: Table identifier (e.g., "orders" or "prod.orders")

        Returns:
            List of matched column identifiers (original case preserved)

        Raises:
            ValueError: If no columns found for the table
        """
        table_lower = table.lower()
        parts = table_lower.split(".")
        matched_columns = []

        if len(parts) == 1:
            # Single part: match on GraphNode.table field (cross-schema)
            for node in self.graph.nodes:
                if node.table and node.table.lower() == table_lower:
                    matched_columns.append(node.identifier)
        else:
            # Two+ parts: prefix match on identifier (schema.table.)
            prefix = table_lower + "."
            for node in self.graph.nodes:
                if node.identifier.lower().startswith(prefix):
                    matched_columns.append(node.identifier)

        if not matched_columns:
            raise ValueError(f"No columns found for table '{table}'")

        return sorted(matched_columns, key=str.lower)

    def _aggregate_table_results(
        self,
        results: List[LineageQueryResult],
        queried_columns: List[str],
        table: str,
        direction: str,
    ) -> LineageQueryResult:
        """
        Aggregate multiple column-level results into a single table-level result.

        Args:
            results: List of per-column LineageQueryResult objects
            queried_columns: List of column identifiers that were queried
            table: The table that was queried
            direction: Query direction ("upstream" or "downstream")

        Returns:
            Aggregated LineageQueryResult with deduplicated nodes
        """
        # Build set of queried column identifiers (lowercase for comparison)
        queried_set = {col.lower() for col in queried_columns}

        # Aggregate nodes by identifier, tracking min hops and combining paths
        node_map: Dict[str, LineageNode] = {}

        for result in results:
            for node in result.related_columns:
                node_key = node.identifier.lower()

                # Skip nodes that are part of the queried table
                if node_key in queried_set:
                    continue

                if node_key not in node_map:
                    # First occurrence - store the node
                    node_map[node_key] = LineageNode(
                        identifier=node.identifier,
                        file_path=node.file_path,
                        query_index=node.query_index,
                        schema_name=node.schema_name,
                        table=node.table,
                        column=node.column,
                        hops=node.hops,
                        output_column=table,  # Use table as the output reference
                        is_root=node.is_root,
                        is_leaf=node.is_leaf,
                        paths=list(node.paths),
                    )
                else:
                    # Merge with existing node
                    existing = node_map[node_key]
                    # Use minimum hops
                    if node.hops < existing.hops:
                        existing.hops = node.hops
                    # Combine paths (avoid duplicates by comparing node lists)
                    existing_path_sets = {tuple(p.nodes) for p in existing.paths}
                    for path in node.paths:
                        if tuple(path.nodes) not in existing_path_sets:
                            existing.paths.append(path)
                            existing_path_sets.add(tuple(path.nodes))

        # Sort aggregated nodes by identifier
        aggregated_nodes = sorted(node_map.values(), key=lambda n: n.identifier.lower())

        return LineageQueryResult(
            query_column=table,
            direction=direction,
            related_columns=aggregated_nodes,
            queried_columns=queried_columns,
            is_table_query=True,
        )

    def find_upstream_table(self, table: str) -> LineageQueryResult:
        """
        Find all upstream (source) columns for all columns in a table.

        Aggregates upstream lineage from all columns belonging to the specified
        table into a single result with deduplicated nodes.

        Args:
            table: Table identifier to analyze (e.g., "orders" or "prod.orders")

        Returns:
            LineageQueryResult with aggregated upstream columns

        Raises:
            ValueError: If no columns found for the table
        """
        # Find all columns in the table
        table_columns = self._find_table_columns(table)

        # Query upstream for each column
        results = []
        for column in table_columns:
            try:
                result = self.find_upstream(column)
                results.append(result)
            except ValueError:
                # Column exists but might not have upstream - skip
                pass

        # Aggregate results
        return self._aggregate_table_results(results, table_columns, table, "upstream")

    def find_downstream_table(self, table: str) -> LineageQueryResult:
        """
        Find all downstream (affected) columns for all columns in a table.

        Aggregates downstream lineage from all columns belonging to the specified
        table into a single result with deduplicated nodes.

        Args:
            table: Table identifier to analyze (e.g., "orders" or "prod.orders")

        Returns:
            LineageQueryResult with aggregated downstream columns

        Raises:
            ValueError: If no columns found for the table
        """
        # Find all columns in the table
        table_columns = self._find_table_columns(table)

        # Query downstream for each column
        results = []
        for column in table_columns:
            try:
                result = self.find_downstream(column)
                results.append(result)
            except ValueError:
                # Column exists but might not have downstream - skip
                pass

        # Aggregate results
        return self._aggregate_table_results(
            results, table_columns, table, "downstream"
        )

    def list_columns(self) -> List[str]:
        """
        List all column identifiers in the graph.

        Returns:
            Sorted list of column identifiers
        """
        return sorted(self.node_map.keys(), key=str.lower)
