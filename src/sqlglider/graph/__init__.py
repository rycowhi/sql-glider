"""Graph-based lineage analysis module for SQL Glider."""

from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
    Manifest,
    ManifestEntry,
)
from sqlglider.graph.builder import GraphBuilder
from sqlglider.graph.merge import GraphMerger, merge_graphs
from sqlglider.graph.query import GraphQuerier, LineageQueryResult
from sqlglider.graph.serialization import (
    load_graph,
    save_graph,
    to_rustworkx,
    from_rustworkx,
)

__all__ = [
    # Models
    "GraphNode",
    "GraphEdge",
    "GraphMetadata",
    "LineageGraph",
    "Manifest",
    "ManifestEntry",
    # Builder
    "GraphBuilder",
    # Merge
    "GraphMerger",
    "merge_graphs",
    # Query
    "GraphQuerier",
    "LineageQueryResult",
    # Serialization
    "load_graph",
    "save_graph",
    "to_rustworkx",
    "from_rustworkx",
]
