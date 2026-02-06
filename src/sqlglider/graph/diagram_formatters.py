"""Diagram formatters for lineage graphs (Mermaid, DOT/Graphviz, and Plotly)."""

import json
import re
from collections import defaultdict
from typing import Any, Optional, Set

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
    # Include all queried columns (supports table-level queries)
    nodes.update(result.queried_columns)
    return nodes


def _get_queried_column_ids(result: LineageQueryResult) -> Set[str]:
    """Get the set of queried column identifiers for highlighting.

    For column-level queries, returns just the single queried column.
    For table-level queries, returns all columns in the queried table.

    Args:
        result: Query result

    Returns:
        Set of column identifiers to highlight as "queried"
    """
    return set(result.queried_columns)


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

        # Add edges with file path labels
        for edge in graph.edges:
            src = _sanitize_mermaid_id(edge.source_node)
            tgt = _sanitize_mermaid_id(edge.target_node)
            # Extract filename from path
            file_name = (
                edge.file_path.split("/")[-1].split("\\")[-1] if edge.file_path else ""
            )
            if file_name:
                lines.append(f"    {src} -->|{file_name}| {tgt}")
            else:
                lines.append(f"    {src} --> {tgt}")

        return "\n".join(lines)

    @staticmethod
    def format_query_result(
        result: LineageQueryResult,
        graph: Optional[LineageGraph] = None,
    ) -> str:
        """Format query result as a Mermaid flowchart with styling.

        The queried column is highlighted in amber, root nodes in teal,
        and leaf nodes in violet. A legend subgraph is included.

        Args:
            result: LineageQueryResult from upstream/downstream query
            graph: Optional LineageGraph for edge file path labels

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

        # Build edge file path lookup if graph is provided
        edge_file_paths: dict[tuple[str, str], str] = {}
        if graph:
            for e in graph.edges:
                edge_file_paths[(e.source_node, e.target_node)] = e.file_path

        # Declare nodes
        for identifier in sorted(all_nodes):
            node_id = _sanitize_mermaid_id(identifier)
            lines.append(f'    {node_id}["{identifier}"]')

        # Add edges with optional file path labels
        for src, tgt in sorted(edges):
            src_id = _sanitize_mermaid_id(src)
            tgt_id = _sanitize_mermaid_id(tgt)
            file_path = edge_file_paths.get((src, tgt), "")
            file_name = file_path.split("/")[-1].split("\\")[-1] if file_path else ""
            if file_name:
                lines.append(f"    {src_id} -->|{file_name}| {tgt_id}")
            else:
                lines.append(f"    {src_id} --> {tgt_id}")

        # Style directives
        # Get all queried column IDs (supports table-level queries)
        queried_ids = {
            _sanitize_mermaid_id(col) for col in _get_queried_column_ids(result)
        }
        for qid in sorted(queried_ids):
            lines.append(
                f"    style {qid} fill:{QUERIED_FILL},stroke:{QUERIED_STROKE},stroke-width:3px"
            )

        root_ids = set()
        leaf_ids = set()
        for node in result.related_columns:
            if node.is_root:
                root_ids.add(_sanitize_mermaid_id(node.identifier))
            if node.is_leaf:
                leaf_ids.add(_sanitize_mermaid_id(node.identifier))

        for rid in sorted(root_ids):
            if rid not in queried_ids:
                lines.append(f"    style {rid} fill:{ROOT_FILL},stroke:{ROOT_STROKE}")

        for lid in sorted(leaf_ids):
            if lid not in queried_ids and lid not in root_ids:
                lines.append(f"    style {lid} fill:{LEAF_FILL},stroke:{LEAF_STROKE}")

        # Legend
        lines.append("")
        lines.append("    subgraph Legend")
        queried_label = (
            "Queried Table Columns" if result.is_table_query else "Queried Column"
        )
        lines.append(f'        legend_queried["{queried_label}"]')
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
    def format_query_result(
        result: LineageQueryResult,
        graph: Optional[LineageGraph] = None,
    ) -> str:
        """Format query result as a Mermaid diagram in a markdown code block.

        Args:
            result: LineageQueryResult from upstream/downstream query
            graph: Optional LineageGraph for edge file path labels

        Returns:
            Markdown string with fenced Mermaid diagram
        """
        mermaid = MermaidFormatter.format_query_result(result, graph=graph)
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

        # Get all queried column IDs (supports table-level queries)
        queried_cols = _get_queried_column_ids(result)

        if not result.related_columns:
            # Show just the queried column(s)
            for col in sorted(queried_cols):
                qid = _quote_dot_id(col)
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
            if identifier in queried_cols:
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
        queried_label = (
            "Queried Table Columns" if result.is_table_query else "Queried Column"
        )
        lines.append(
            f'        legend_queried [label="{queried_label}", style="rounded,filled", fillcolor="{QUERIED_FILL}"];'
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
    x_spacing: float = 250.0,
    y_spacing: float = 100.0,
) -> dict[str, tuple[float, float]]:
    """Compute layered layout positions for nodes using Graphviz's dot algorithm.

    Uses Graphviz's dot layout engine (Sugiyama algorithm) which minimizes edge
    crossings by using the barycenter heuristic to optimize node ordering within
    each layer.

    Positions nodes in layers from left to right based on their dependencies.
    Nodes with no incoming edges are placed in layer 0, their dependents in
    layer 1, etc.

    Args:
        nodes: List of node identifiers
        edges: List of (source, target) edge tuples
        x_spacing: Horizontal spacing between layers (default: 250 pixels)
        y_spacing: Vertical spacing between nodes in the same layer (default: 100 pixels)

    Returns:
        Dictionary mapping node identifiers to (x, y) positions
    """
    if not nodes:
        return {}

    try:
        import pygraphviz  # noqa: F401  # type: ignore[import-not-found]

        return _compute_graphviz_layout(nodes, edges, x_spacing, y_spacing)
    except ImportError:
        # Fallback to simple layered layout if pygraphviz not available
        return _compute_simple_layered_layout(nodes, edges, x_spacing, y_spacing)


def _compute_graphviz_layout(
    nodes: list[str],
    edges: list[tuple[str, str]],
    x_spacing: float = 250.0,
    y_spacing: float = 100.0,
) -> dict[str, tuple[float, float]]:
    """Compute layout using Graphviz's dot algorithm with crossing minimization.

    Args:
        nodes: List of node identifiers
        edges: List of (source, target) edge tuples
        x_spacing: Horizontal spacing between layers
        y_spacing: Vertical spacing between nodes in the same layer

    Returns:
        Dictionary mapping node identifiers to (x, y) positions
    """
    import pygraphviz as pgv  # type: ignore[import-not-found]

    # Create directed graph
    g = pgv.AGraph(directed=True, rankdir="LR")

    # Set graph attributes for spacing
    g.graph_attr["ranksep"] = str(x_spacing / 72.0)  # Convert pixels to inches
    g.graph_attr["nodesep"] = str(y_spacing / 72.0)

    # Add nodes
    for node in nodes:
        g.add_node(node)

    # Add edges (only for nodes that exist in our node list)
    node_set = set(nodes)
    for src, tgt in edges:
        if src in node_set and tgt in node_set:
            g.add_edge(src, tgt)

    # Compute layout using dot algorithm
    g.layout(prog="dot")

    # Extract positions
    positions: dict[str, tuple[float, float]] = {}
    for node in nodes:
        n = g.get_node(node)
        # Position is returned as "x,y" string in points (1/72 inch)
        pos_str = n.attr.get("pos", "0,0")
        if pos_str:
            x_str, y_str = pos_str.split(",")
            # Convert from points to our coordinate system
            x = float(x_str)
            y = float(y_str)
            positions[node] = (x, y)

    return positions


def _compute_simple_layered_layout(
    nodes: list[str],
    edges: list[tuple[str, str]],
    x_spacing: float = 250.0,
    y_spacing: float = 100.0,
) -> dict[str, tuple[float, float]]:
    """Fallback simple layered layout using topological ordering.

    Used when pygraphviz is not available. Does not minimize edge crossings.

    Args:
        nodes: List of node identifiers
        edges: List of (source, target) edge tuples
        x_spacing: Horizontal spacing between layers
        y_spacing: Vertical spacing between nodes in the same layer

    Returns:
        Dictionary mapping node identifiers to (x, y) positions
    """
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

    for layer, layer_nodes in layer_groups.items():
        x = layer * x_spacing
        n = len(layer_nodes)
        for i, node in enumerate(sorted(layer_nodes)):
            # Center nodes vertically, spread them out uniformly
            y = (i - (n - 1) / 2) * y_spacing
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

        # Build lookup for edge file paths
        edge_file_paths: dict[tuple[str, str], str] = {}
        for e in graph.edges:
            edge_file_paths[(e.source_node, e.target_node)] = e.file_path

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

        # Build edge traces with hover text for file path
        edge_traces: list[dict[str, Any]] = []
        edge_annotations: list[dict[str, Any]] = []
        for src, tgt in edge_tuples:
            if src in positions and tgt in positions:
                x0, y0 = positions[src]
                x1, y1 = positions[tgt]
                file_path = edge_file_paths.get((src, tgt), "")
                # Extract just the filename for display
                file_name = (
                    file_path.split("/")[-1].split("\\")[-1] if file_path else ""
                )

                edge_traces.append(
                    {
                        "type": "scatter",
                        "x": [x0, x1, None],
                        "y": [y0, y1, None],
                        "mode": "lines",
                        "line": {"width": 1.5, "color": "#888"},
                        "hoverinfo": "text",
                        "hovertext": file_path,
                        "showlegend": False,
                    }
                )

                # Add annotation at midpoint of edge
                mid_x = (x0 + x1) / 2
                mid_y = (y0 + y1) / 2
                edge_annotations.append(
                    {
                        "x": mid_x,
                        "y": mid_y,
                        "text": file_name,
                        "showarrow": False,
                        "font": {"size": 9, "color": "#666"},
                        "bgcolor": "rgba(255,255,255,0.8)",
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
            "textfont": {"size": 11},
            "hoverinfo": "text",
            "marker": {
                "size": 15,
                "color": "#6495ED",
                "line": {"width": 2, "color": "#4169E1"},
            },
            "showlegend": False,
        }

        # Calculate figure dimensions based on graph size
        min_height = 400
        height_per_node = 50
        calculated_height = max(min_height, len(node_ids) * height_per_node)

        figure = {
            "data": edge_traces + [node_trace],
            "layout": {
                "title": {"text": "Lineage Graph"},
                "showlegend": False,
                "hovermode": "closest",
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "height": calculated_height,
                "margin": {"l": 50, "r": 50, "t": 60, "b": 40},
                "annotations": edge_annotations,
            },
        }

        return json.dumps(figure, indent=2)

    @staticmethod
    def format_query_result(
        result: LineageQueryResult,
        graph: Optional[LineageGraph] = None,
    ) -> str:
        """Format query result as a Plotly JSON figure with styling.

        The queried column is highlighted in amber, root nodes in teal,
        and leaf nodes in violet, matching the Mermaid/DOT color scheme.

        Args:
            result: LineageQueryResult from upstream/downstream query
            graph: Optional LineageGraph for edge file path labels

        Returns:
            JSON string representing a Plotly figure specification
        """
        PlotlyFormatter._check_plotly_available()

        all_nodes = _collect_query_nodes(result)
        edges = _collect_query_edges(result)

        # Build edge file path lookup if graph is provided
        edge_file_paths: dict[tuple[str, str], str] = {}
        if graph:
            for e in graph.edges:
                edge_file_paths[(e.source_node, e.target_node)] = e.file_path

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

        # Get all queried column IDs (supports table-level queries)
        queried_cols = _get_queried_column_ids(result)

        # Determine node colors
        node_colors: list[str] = []
        for node in node_list:
            if node in queried_cols:
                node_colors.append(QUERIED_FILL)
            elif node in root_ids:
                node_colors.append(ROOT_FILL)
            elif node in leaf_ids:
                node_colors.append(LEAF_FILL)
            else:
                node_colors.append("#6495ED")  # Default blue

        # Build edge traces with optional file path labels
        edge_traces: list[dict[str, Any]] = []
        edge_annotations: list[dict[str, Any]] = []
        for src, tgt in sorted(edges):
            if src in positions and tgt in positions:
                x0, y0 = positions[src]
                x1, y1 = positions[tgt]
                file_path = edge_file_paths.get((src, tgt), "")
                file_name = (
                    file_path.split("/")[-1].split("\\")[-1] if file_path else ""
                )

                edge_traces.append(
                    {
                        "type": "scatter",
                        "x": [x0, x1, None],
                        "y": [y0, y1, None],
                        "mode": "lines",
                        "line": {"width": 1.5, "color": "#888"},
                        "hoverinfo": "text" if file_path else "none",
                        "hovertext": file_path if file_path else None,
                        "showlegend": False,
                    }
                )

                # Add annotation at midpoint of edge if we have file path info
                if file_name:
                    mid_x = (x0 + x1) / 2
                    mid_y = (y0 + y1) / 2
                    edge_annotations.append(
                        {
                            "x": mid_x,
                            "y": mid_y,
                            "text": file_name,
                            "showarrow": False,
                            "font": {"size": 9, "color": "#666"},
                            "bgcolor": "rgba(255,255,255,0.8)",
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
            "textfont": {"size": 11},
            "hoverinfo": "text",
            "marker": {
                "size": 15,
                "color": node_colors,
                "line": {"width": 2, "color": "#333"},
            },
            "showlegend": False,
        }

        # Build legend traces (invisible markers for legend display)
        queried_legend_label = (
            "Queried Table Columns" if result.is_table_query else "Queried Column"
        )
        legend_traces: list[dict[str, Any]] = [
            {
                "type": "scatter",
                "x": [None],
                "y": [None],
                "mode": "markers",
                "marker": {"size": 12, "color": QUERIED_FILL},
                "name": queried_legend_label,
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
        entity_type = "Table" if result.is_table_query else ""
        title = (
            f"{direction_label} Lineage: {entity_type} {result.query_column}".replace(
                "  ", " "
            ).strip()
        )

        # Calculate figure dimensions based on graph size
        min_height = 400
        height_per_node = 50
        calculated_height = max(min_height, len(node_list) * height_per_node)

        layout: dict[str, Any] = {
            "title": {"text": title},
            "showlegend": True,
            "legend": {"x": 1, "y": 1, "xanchor": "right"},
            "hovermode": "closest",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
            "height": calculated_height,
            "margin": {"l": 50, "r": 50, "t": 60, "b": 40},
        }

        # Add edge annotations if we have file path info
        if edge_annotations:
            layout["annotations"] = edge_annotations

        figure = {
            "data": edge_traces + [node_trace] + legend_traces,
            "layout": layout,
        }

        return json.dumps(figure, indent=2)
