"""Graph merging functionality."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

import rustworkx as rx

from sqlglider.global_models import NodeFormat
from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
)
from sqlglider.graph.serialization import load_graph


class GraphMerger:
    """Merge multiple lineage graphs into one."""

    def __init__(self):
        """Initialize the merger."""
        self.merged_graph: rx.PyDiGraph = rx.PyDiGraph()
        self._node_map: Dict[str, int] = {}  # identifier -> node index
        self._source_files: Set[str] = set()
        self._edge_set: Set[tuple] = set()  # (source, target) for dedup

    def add_graph(self, graph: LineageGraph) -> "GraphMerger":
        """
        Add a graph to be merged.

        Nodes are deduplicated by identifier (first occurrence wins).
        Edges are deduplicated by (source_node, target_node) pair.

        Args:
            graph: LineageGraph to add

        Returns:
            self for method chaining
        """
        self._source_files.update(graph.metadata.source_files)

        # Add nodes (deduplicate by identifier)
        for node in graph.nodes:
            if node.identifier not in self._node_map:
                idx = self.merged_graph.add_node(node.model_dump())
                self._node_map[node.identifier] = idx

        # Add edges (deduplicate by source-target pair)
        for edge in graph.edges:
            edge_key = (edge.source_node, edge.target_node)
            if edge_key not in self._edge_set:
                source_idx = self._node_map.get(edge.source_node)
                target_idx = self._node_map.get(edge.target_node)
                if source_idx is not None and target_idx is not None:
                    self.merged_graph.add_edge(
                        source_idx, target_idx, edge.model_dump()
                    )
                    self._edge_set.add(edge_key)

        return self

    def add_file(self, graph_path: Path) -> "GraphMerger":
        """
        Add a graph from a JSON file.

        Args:
            graph_path: Path to graph JSON file

        Returns:
            self for method chaining

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is not valid graph JSON
        """
        graph = load_graph(graph_path)
        return self.add_graph(graph)

    def add_files(self, graph_paths: List[Path]) -> "GraphMerger":
        """
        Add multiple graphs from JSON files.

        Args:
            graph_paths: List of paths to graph JSON files

        Returns:
            self for method chaining
        """
        for path in graph_paths:
            self.add_file(path)
        return self

    def merge(self) -> LineageGraph:
        """
        Build the merged graph.

        Returns:
            Merged LineageGraph with combined nodes and edges
        """
        nodes = [
            GraphNode(**self.merged_graph[idx])
            for idx in self.merged_graph.node_indices()
        ]
        edges = [
            GraphEdge(**self.merged_graph.get_edge_data_by_index(idx))
            for idx in self.merged_graph.edge_indices()
        ]

        metadata = GraphMetadata(
            node_format=NodeFormat.QUALIFIED,  # Merged graphs use qualified format
            default_dialect="spark",
            created_at=datetime.now(timezone.utc).isoformat(),
            source_files=sorted(self._source_files),
            total_nodes=len(nodes),
            total_edges=len(edges),
        )

        return LineageGraph(metadata=metadata, nodes=nodes, edges=edges)


def merge_graphs(graph_paths: List[Path]) -> LineageGraph:
    """
    Convenience function to merge multiple graph files.

    Args:
        graph_paths: List of paths to graph JSON files

    Returns:
        Merged LineageGraph
    """
    merger = GraphMerger()
    for path in graph_paths:
        merger.add_file(path)
    return merger.merge()
