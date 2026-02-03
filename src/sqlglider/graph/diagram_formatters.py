"""Diagram formatters for lineage graphs (Mermaid, DOT/Graphviz, and Plotly)."""

import json
import re
from collections import defaultdict
from typing import Any, Set

from sqlglider.graph.models import LineageGraph
from sqlglider.graph.query import LineageQueryResult

# Color palette (muted jewel tones for light/dark mode compatibility)
QUERIED_FILL = "#e6a843"
QUERIED_STROKE = "#b8860b"
ROOT_FILL = "#4ecdc4"
ROOT_STROKE = "#2b9e96"
LEAF_FILL = "#c084fc"
LEAF_STROKE = "#7c3aed"


def _sanitize_mermaid_id(identifier: str) -> str:
    """Sanitize an identifier for use as a Mermaid node ID.

    Replaces non-alphanumeric characters with underscores.

    Args:
        identifier: Raw node identifier (e.g., "schema.table.column")

    Returns:
        Sanitized ID safe for Mermaid syntax
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", identifier)


def _quote_dot_id(identifier: str) -> str:
    """Quote an identifier for use in DOT syntax.

    Args:
        identifier: Raw node identifier

    Returns:
        Double-quoted identifier with internal quotes escaped
    """
    escaped = identifier.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _collect_query_edges(result: LineageQueryResult) -> Set[tuple[str, str]]:
    """Extract unique directed edges from all paths in a query result.

    Args:
        result: Query result containing paths

    Returns:
        Set of (source, target) identifier pairs
    """
    edges: Set[tuple[str, str]] = set()
    for node in result.related_columns:
        for path in node.paths:
            for i in range(len(path.nodes) - 1):
                edges.add((path.nodes[i], path.nodes[i + 1]))
    return edges


def _collect_query_nodes(result: LineageQueryResult) -> Set[str]:
    """Extract all unique node identifiers from query result paths.

    Args:
        result: Query result containing paths

    Returns:
        Set of node identifiers
    """
    nodes: Set[str] = set()
    for node in result.related_columns:
        for path in node.paths:
            nodes.update(path.nodes)
    # Always include the queried column itself
    nodes.add(result.query_column)
    return nodes


class MermaidFormatter:
    """Format lineage graphs and query results as Mermaid diagrams."""

    @staticmethod
    def format_full_graph(graph: LineageGraph) -> str:
        """Format complete lineage graph as a Mermaid flowchart.

        Args:
            graph: LineageGraph with all nodes and edges

        Returns:
            Mermaid diagram string (flowchart TD syntax)
        """
        lines = ["flowchart TD"]

        if not graph.nodes and not graph.edges:
            return "\n".join(lines)

        # Declare nodes with labels
        for node in graph.nodes:
            node_id = _sanitize_mermaid_id(node.identifier)
            lines.append(f'    {node_id}["{node.identifier}"]')

        # Add edges
        for edge in graph.edges:
            src = _sanitize_mermaid_id(edge.source_node)
            tgt = _sanitize_mermaid_id(edge.target_node)
            lines.append(f"    {src} --> {tgt}")

        return "\n".join(lines)

    @staticmethod
    def format_query_result(result: LineageQueryResult) -> str:
        """Format query result as a Mermaid flowchart with styling.

        The queried column is highlighted in amber, root nodes in teal,
        and leaf nodes in violet. A legend subgraph is included.

        Args:
            result: LineageQueryResult from upstream/downstream query

        Returns:
            Mermaid diagram string with style directives and legend
        """
        lines = ["flowchart TD"]

        if not result.related_columns:
            # Show just the queried node
            node_id = _sanitize_mermaid_id(result.query_column)
            lines.append(f'    {node_id}["{result.query_column}"]')
            return "\n".join(lines)

        all_nodes = _collect_query_nodes(result)
        edges = _collect_query_edges(result)

        # Declare nodes
        for identifier in sorted(all_nodes):
            node_id = _sanitize_mermaid_id(identifier)
            lines.append(f'    {node_id}["{identifier}"]')

        # Add edges
        for src, tgt in sorted(edges):
            lines.append(
                f"    {_sanitize_mermaid_id(src)} --> {_sanitize_mermaid_id(tgt)}"
            )

        # Style directives
        queried_id = _sanitize_mermaid_id(result.query_column)
        lines.append(
            f"    style {queried_id} fill:{QUERIED_FILL},stroke:{QUERIED_STROKE},stroke-width:3px"
        )

        root_ids = set()
        leaf_ids = set()
        for node in result.related_columns:
            if node.is_root:
                root_ids.add(_sanitize_mermaid_id(node.identifier))
            if node.is_leaf:
                leaf_ids.add(_sanitize_mermaid_id(node.identifier))

        for rid in sorted(root_ids):
            if rid != queried_id:
                lines.append(f"    style {rid} fill:{ROOT_FILL},stroke:{ROOT_STROKE}")

        for lid in sorted(leaf_ids):
            if lid != queried_id and lid not in root_ids:
                lines.append(f"    style {lid} fill:{LEAF_FILL},stroke:{LEAF_STROKE}")

        # Legend
        lines.append("")
        lines.append("    subgraph Legend")
        lines.append('        legend_queried["Queried Column"]')
        lines.append('        legend_root["Root (no upstream)"]')
        lines.append('        legend_leaf["Leaf (no downstream)"]')
        lines.append("    end")
        lines.append(
            f"    style legend_queried fill:{QUERIED_FILL},stroke:{QUERIED_STROKE},stroke-width:3px"
        )
        lines.append(f"    style legend_root fill:{ROOT_FILL},stroke:{ROOT_STROKE}")
        lines.append(f"    style legend_leaf fill:{LEAF_FILL},stroke:{LEAF_STROKE}")

        return "\n".join(lines)


class MermaidMarkdownFormatter:
    """Format lineage graphs and query results as Mermaid diagrams wrapped in markdown code fences."""

    @staticmethod
    def format_full_graph(graph: LineageGraph) -> str:
        """Format complete lineage graph as a Mermaid diagram in a markdown code block.

        Args:
            graph: LineageGraph with all nodes and edges

        Returns:
            Markdown string with fenced Mermaid diagram
        """
        mermaid = MermaidFormatter.format_full_graph(graph)
        return f"```mermaid\n{mermaid}\n```"

    @staticmethod
    def format_query_result(result: LineageQueryResult) -> str:
        """Format query result as a Mermaid diagram in a markdown code block.

        Args:
            result: LineageQueryResult from upstream/downstream query

        Returns:
            Markdown string with fenced Mermaid diagram
        """
        mermaid = MermaidFormatter.format_query_result(result)
        return f"```mermaid\n{mermaid}\n```"


class DotFormatter:
    """Format lineage graphs and query results as DOT (Graphviz) diagrams."""

    @staticmethod
    def format_full_graph(graph: LineageGraph) -> str:
        """Format complete lineage graph as a DOT digraph.

        Args:
            graph: LineageGraph with all nodes and edges

        Returns:
            DOT diagram string
        """
        lines = [
            "digraph lineage {",
            "    rankdir=LR;",
            "    node [shape=box, style=rounded];",
        ]

        if not graph.nodes and not graph.edges:
            lines.append("}")
            return "\n".join(lines)

        # Declare nodes
        for node in graph.nodes:
            lines.append(f"    {_quote_dot_id(node.identifier)};")

        # Add edges
        for edge in graph.edges:
            src = _quote_dot_id(edge.source_node)
            tgt = _quote_dot_id(edge.target_node)
            lines.append(f"    {src} -> {tgt};")

        lines.append("}")
        return "\n".join(lines)

    @staticmethod
    def format_query_result(result: LineageQueryResult) -> str:
        """Format query result as a DOT digraph with styling.

        The queried column is highlighted in amber, root nodes in teal,
        and leaf nodes in violet. A legend subgraph is included.

        Args:
            result: LineageQueryResult from upstream/downstream query

        Returns:
            DOT diagram string with node attributes and legend
        """
        lines = [
            "digraph lineage {",
            "    rankdir=LR;",
            "    node [shape=box, style=rounded];",
        ]

        if not result.related_columns:
            qid = _quote_dot_id(result.query_column)
            lines.append(
                f'    {qid} [style="rounded,filled", fillcolor="{QUERIED_FILL}"];'
            )
            lines.append("}")
            return "\n".join(lines)

        all_nodes = _collect_query_nodes(result)
        edges = _collect_query_edges(result)

        # Build styling lookup
        root_ids: Set[str] = set()
        leaf_ids: Set[str] = set()
        for node in result.related_columns:
            if node.is_root:
                root_ids.add(node.identifier)
            if node.is_leaf:
                leaf_ids.add(node.identifier)

        # Declare nodes with styling
        for identifier in sorted(all_nodes):
            qid = _quote_dot_id(identifier)
            if identifier == result.query_column:
                lines.append(
                    f'    {qid} [style="rounded,filled", fillcolor="{QUERIED_FILL}"];'
                )
            elif identifier in root_ids:
                lines.append(
                    f'    {qid} [style="rounded,filled", fillcolor="{ROOT_FILL}"];'
                )
            elif identifier in leaf_ids:
                lines.append(
                    f'    {qid} [style="rounded,filled", fillcolor="{LEAF_FILL}"];'
                )
            else:
                lines.append(f"    {qid};")

        # Add edges
        for src, tgt in sorted(edges):
            lines.append(f"    {_quote_dot_id(src)} -> {_quote_dot_id(tgt)};")

        # Legend
        lines.append("")
        lines.append("    subgraph cluster_legend {")
        lines.append('        label="Legend";')
        lines.append("        style=dashed;")
        lines.append(
            f'        legend_queried [label="Queried Column", style="rounded,filled", fillcolor="{QUERIED_FILL}"];'
        )
        lines.append(
            f'        legend_root [label="Root (no upstream)", style="rounded,filled", fillcolor="{ROOT_FILL}"];'
        )
        lines.append(
            f'        legend_leaf [label="Leaf (no downstream)", style="rounded,filled", fillcolor="{LEAF_FILL}"];'
        )
        lines.append("        legend_queried -> legend_root [style=invis];")
        lines.append("        legend_root -> legend_leaf [style=invis];")
        lines.append("    }")

        lines.append("}")
        return "\n".join(lines)


def _compute_layered_layout(
    nodes: list[str],
    edges: list[tuple[str, str]],
) -> dict[str, tuple[float, float]]:
    """Compute layered layout positions for nodes using topological ordering.

    Positions nodes in layers from left to right based on their dependencies.
    Nodes with no incoming edges are placed in layer 0, their dependents in
    layer 1, etc.

    Args:
        nodes: List of node identifiers
        edges: List of (source, target) edge tuples

    Returns:
        Dictionary mapping node identifiers to (x, y) positions
    """
    if not nodes:
        return {}

    # Build adjacency structures
    incoming: dict[str, set[str]] = defaultdict(set)
    outgoing: dict[str, set[str]] = defaultdict(set)
    for src, tgt in edges:
        outgoing[src].add(tgt)
        incoming[tgt].add(src)

    # Assign layers via modified Kahn's algorithm
    layers: dict[str, int] = {}
    node_set = set(nodes)

    # Start with nodes that have no incoming edges (roots)
    current_layer = [n for n in nodes if not incoming[n]]
    if not current_layer:
        # Cycle detected or all nodes have incoming edges, use first node
        current_layer = [nodes[0]]

    layer_num = 0
    while current_layer:
        next_layer = []
        for node in current_layer:
            if node not in layers:
                layers[node] = layer_num
            for child in outgoing[node]:
                if child in node_set and child not in layers:
                    # Check if all parents are assigned
                    if all(p in layers for p in incoming[child]):
                        next_layer.append(child)
        layer_num += 1
        current_layer = next_layer

    # Assign any remaining unvisited nodes to the last layer
    for node in nodes:
        if node not in layers:
            layers[node] = layer_num

    # Group nodes by layer for vertical positioning
    layer_groups: dict[int, list[str]] = defaultdict(list)
    for node, layer in layers.items():
        layer_groups[layer].append(node)

    # Compute positions: x based on layer, y spread vertically within layer
    positions: dict[str, tuple[float, float]] = {}
    max_layer = max(layers.values()) if layers else 0
    x_spacing = 1.0 if max_layer == 0 else 1.0

    for layer, layer_nodes in layer_groups.items():
        x = layer * x_spacing
        n = len(layer_nodes)
        for i, node in enumerate(sorted(layer_nodes)):
            # Center nodes vertically, spread them out
            y = (i - (n - 1) / 2) * 0.5
            positions[node] = (x, y)

    return positions


class PlotlyFormatter:
    """Format lineage graphs as Plotly JSON figure specifications.

    Generates JSON that can be loaded into Plotly/Dash applications using
    plotly.io.from_json() or directly into dcc.Graph components.

    Requires the 'plotly' optional dependency: pip install sql-glider[plotly]
    """

    @staticmethod
    def _check_plotly_available() -> None:
        """Check if plotly is installed, raise ImportError if not."""
        try:
            import plotly  # noqa: F401
        except ImportError:
            raise ImportError(
                "Plotly is required for this output format. "
                "Install it with: pip install sql-glider[plotly]"
            )

    @staticmethod
    def format_full_graph(graph: LineageGraph) -> str:
        """Format complete lineage graph as a Plotly JSON figure.

        Args:
            graph: LineageGraph with all nodes and edges

        Returns:
            JSON string representing a Plotly figure specification
        """
        PlotlyFormatter._check_plotly_available()

        node_ids = [n.identifier for n in graph.nodes]
        edge_tuples = [(e.source_node, e.target_node) for e in graph.edges]

        if not node_ids:
            # Empty graph
            figure: dict[str, Any] = {
                "data": [],
                "layout": {
                    "title": {"text": "Lineage Graph"},
                    "showlegend": False,
                    "xaxis": {"visible": False},
                    "yaxis": {"visible": False},
                },
            }
            return json.dumps(figure, indent=2)

        positions = _compute_layered_layout(node_ids, edge_tuples)

        # Build edge traces (one trace per edge for simplicity)
        edge_traces: list[dict[str, Any]] = []
        for src, tgt in edge_tuples:
            if src in positions and tgt in positions:
                x0, y0 = positions[src]
                x1, y1 = positions[tgt]
                edge_traces.append(
                    {
                        "type": "scatter",
                        "x": [x0, x1, None],
                        "y": [y0, y1, None],
                        "mode": "lines",
                        "line": {"width": 1, "color": "#888"},
                        "hoverinfo": "none",
                        "showlegend": False,
                    }
                )

        # Build node trace
        node_x = [positions[n][0] for n in node_ids if n in positions]
        node_y = [positions[n][1] for n in node_ids if n in positions]
        node_text = [n for n in node_ids if n in positions]

        node_trace: dict[str, Any] = {
            "type": "scatter",
            "x": node_x,
            "y": node_y,
            "mode": "markers+text",
            "text": node_text,
            "textposition": "top center",
            "hoverinfo": "text",
            "marker": {
                "size": 20,
                "color": "#6495ED",
                "line": {"width": 2, "color": "#4169E1"},
            },
            "showlegend": False,
        }

        figure = {
            "data": edge_traces + [node_trace],
            "layout": {
                "title": {"text": "Lineage Graph"},
                "showlegend": False,
                "hovermode": "closest",
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "margin": {"l": 40, "r": 40, "t": 60, "b": 40},
            },
        }

        return json.dumps(figure, indent=2)

    @staticmethod
    def format_query_result(result: LineageQueryResult) -> str:
        """Format query result as a Plotly JSON figure with styling.

        The queried column is highlighted in amber, root nodes in teal,
        and leaf nodes in violet, matching the Mermaid/DOT color scheme.

        Args:
            result: LineageQueryResult from upstream/downstream query

        Returns:
            JSON string representing a Plotly figure specification
        """
        PlotlyFormatter._check_plotly_available()

        all_nodes = _collect_query_nodes(result)
        edges = _collect_query_edges(result)

        if not all_nodes:
            # Should not happen, but handle gracefully
            figure: dict[str, Any] = {
                "data": [],
                "layout": {
                    "title": {"text": f"Lineage: {result.query_column}"},
                    "showlegend": False,
                    "xaxis": {"visible": False},
                    "yaxis": {"visible": False},
                },
            }
            return json.dumps(figure, indent=2)

        node_list = sorted(all_nodes)
        edge_list = list(edges)
        positions = _compute_layered_layout(node_list, edge_list)

        # Build styling lookup
        root_ids: set[str] = set()
        leaf_ids: set[str] = set()
        for node in result.related_columns:
            if node.is_root:
                root_ids.add(node.identifier)
            if node.is_leaf:
                leaf_ids.add(node.identifier)

        # Determine node colors
        node_colors: list[str] = []
        for node in node_list:
            if node == result.query_column:
                node_colors.append(QUERIED_FILL)
            elif node in root_ids:
                node_colors.append(ROOT_FILL)
            elif node in leaf_ids:
                node_colors.append(LEAF_FILL)
            else:
                node_colors.append("#6495ED")  # Default blue

        # Build edge traces
        edge_traces: list[dict[str, Any]] = []
        for src, tgt in sorted(edges):
            if src in positions and tgt in positions:
                x0, y0 = positions[src]
                x1, y1 = positions[tgt]
                edge_traces.append(
                    {
                        "type": "scatter",
                        "x": [x0, x1, None],
                        "y": [y0, y1, None],
                        "mode": "lines",
                        "line": {"width": 1.5, "color": "#888"},
                        "hoverinfo": "none",
                        "showlegend": False,
                    }
                )

        # Build node trace
        node_x = [positions[n][0] for n in node_list if n in positions]
        node_y = [positions[n][1] for n in node_list if n in positions]

        node_trace: dict[str, Any] = {
            "type": "scatter",
            "x": node_x,
            "y": node_y,
            "mode": "markers+text",
            "text": node_list,
            "textposition": "top center",
            "hoverinfo": "text",
            "marker": {
                "size": 20,
                "color": node_colors,
                "line": {"width": 2, "color": "#333"},
            },
            "showlegend": False,
        }

        # Build legend traces (invisible markers for legend display)
        legend_traces: list[dict[str, Any]] = [
            {
                "type": "scatter",
                "x": [None],
                "y": [None],
                "mode": "markers",
                "marker": {"size": 12, "color": QUERIED_FILL},
                "name": "Queried Column",
                "showlegend": True,
            },
            {
                "type": "scatter",
                "x": [None],
                "y": [None],
                "mode": "markers",
                "marker": {"size": 12, "color": ROOT_FILL},
                "name": "Root (no upstream)",
                "showlegend": True,
            },
            {
                "type": "scatter",
                "x": [None],
                "y": [None],
                "mode": "markers",
                "marker": {"size": 12, "color": LEAF_FILL},
                "name": "Leaf (no downstream)",
                "showlegend": True,
            },
        ]

        direction_label = "Upstream" if result.direction == "upstream" else "Downstream"
        title = f"{direction_label} Lineage: {result.query_column}"

        figure = {
            "data": edge_traces + [node_trace] + legend_traces,
            "layout": {
                "title": {"text": title},
                "showlegend": True,
                "legend": {"x": 1, "y": 1, "xanchor": "right"},
                "hovermode": "closest",
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "margin": {"l": 40, "r": 40, "t": 60, "b": 40},
            },
        }

        return json.dumps(figure, indent=2)
