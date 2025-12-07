"""Graph query functionality for upstream/downstream analysis."""

from pathlib import Path
from typing import List, Optional, Set

import rustworkx as rx

from sqlglider.graph.models import GraphNode, LineageGraph
from sqlglider.graph.serialization import load_graph, to_rustworkx


class LineageQueryResult:
    """Result of a lineage query."""

    def __init__(
        self,
        query_column: str,
        direction: str,  # "upstream" or "downstream"
        related_columns: List[GraphNode],
    ):
        """
        Initialize query result.

        Args:
            query_column: The column that was queried
            direction: Query direction ("upstream" or "downstream")
            related_columns: List of related column nodes
        """
        self.query_column = query_column
        self.direction = direction
        self.related_columns = related_columns

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

        Uses rustworkx.ancestors() to find all nodes that have a path
        leading to the specified column.

        Args:
            column: Column identifier to analyze

        Returns:
            LineageQueryResult with upstream columns

        Raises:
            ValueError: If column not found in graph
        """
        # Case-insensitive lookup
        matched_column = self._find_column(column)
        if matched_column is None:
            raise ValueError(f"Column '{column}' not found in graph")

        node_idx = self.node_map[matched_column]

        # Get all ancestors (upstream nodes)
        ancestor_indices: Set[int] = rx.ancestors(self.rx_graph, node_idx)

        # Convert to GraphNode objects, sorted by identifier
        upstream_columns = sorted(
            [GraphNode(**self.rx_graph[idx]) for idx in ancestor_indices],
            key=lambda n: n.identifier.lower(),
        )

        return LineageQueryResult(
            query_column=matched_column,
            direction="upstream",
            related_columns=upstream_columns,
        )

    def find_downstream(self, column: str) -> LineageQueryResult:
        """
        Find all downstream (affected) columns for a given column.

        Uses rustworkx.descendants() to find all nodes that have a path
        from the specified column.

        Args:
            column: Column identifier to analyze

        Returns:
            LineageQueryResult with downstream columns

        Raises:
            ValueError: If column not found in graph
        """
        # Case-insensitive lookup
        matched_column = self._find_column(column)
        if matched_column is None:
            raise ValueError(f"Column '{column}' not found in graph")

        node_idx = self.node_map[matched_column]

        # Get all descendants (downstream nodes)
        descendant_indices: Set[int] = rx.descendants(self.rx_graph, node_idx)

        # Convert to GraphNode objects, sorted by identifier
        downstream_columns = sorted(
            [GraphNode(**self.rx_graph[idx]) for idx in descendant_indices],
            key=lambda n: n.identifier.lower(),
        )

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

    def list_columns(self) -> List[str]:
        """
        List all column identifiers in the graph.

        Returns:
            Sorted list of column identifiers
        """
        return sorted(self.node_map.keys(), key=str.lower)
