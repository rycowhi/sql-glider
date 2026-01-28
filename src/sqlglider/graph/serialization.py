"""Serialization and deserialization for lineage graphs."""

from pathlib import Path
from typing import Dict, Tuple

import rustworkx as rx

from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
)


def save_graph(graph: LineageGraph, output_path: Path) -> None:
    """
    Save a LineageGraph to a JSON file.

    Args:
        graph: LineageGraph to save
        output_path: Output file path
    """
    output_path.write_text(
        graph.model_dump_json(indent=2),
        encoding="utf-8",
    )


def load_graph(input_path: Path) -> LineageGraph:
    """
    Load a LineageGraph from a JSON file.

    Args:
        input_path: Input file path

    Returns:
        Loaded LineageGraph

    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If file content is invalid JSON or doesn't match schema
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Graph file not found: {input_path}")

    content = input_path.read_text(encoding="utf-8")
    return LineageGraph.model_validate_json(content)


def to_rustworkx(graph: LineageGraph) -> Tuple[rx.PyDiGraph, Dict[str, int]]:
    """
    Convert a LineageGraph to a rustworkx PyDiGraph.

    Args:
        graph: LineageGraph to convert

    Returns:
        Tuple of (PyDiGraph, node_identifier_to_index_map)
    """
    rx_graph: rx.PyDiGraph = rx.PyDiGraph()
    node_map: Dict[str, int] = {}

    # Add nodes (use lowercase keys for case-insensitive lookup)
    for node in graph.nodes:
        idx = rx_graph.add_node(node.model_dump())
        node_map[node.identifier.lower()] = idx

    # Add edges (use lowercase for lookup to match node keys)
    for edge in graph.edges:
        source_idx = node_map.get(edge.source_node.lower())
        target_idx = node_map.get(edge.target_node.lower())
        if source_idx is not None and target_idx is not None:
            rx_graph.add_edge(source_idx, target_idx, edge.model_dump())

    return rx_graph, node_map


def from_rustworkx(
    rx_graph: rx.PyDiGraph,
    metadata: GraphMetadata,
) -> LineageGraph:
    """
    Convert a rustworkx PyDiGraph to a LineageGraph.

    Args:
        rx_graph: rustworkx directed graph
        metadata: Graph metadata to include

    Returns:
        LineageGraph with nodes and edges from the rustworkx graph
    """
    nodes = [GraphNode(**rx_graph[idx]) for idx in rx_graph.node_indices()]
    edges = [
        GraphEdge(**rx_graph.get_edge_data_by_index(idx))
        for idx in rx_graph.edge_indices()
    ]

    # Update metadata counts
    metadata.total_nodes = len(nodes)
    metadata.total_edges = len(edges)

    return LineageGraph(
        metadata=metadata,
        nodes=nodes,
        edges=edges,
    )
