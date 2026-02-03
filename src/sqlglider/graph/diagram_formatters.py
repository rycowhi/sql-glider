"""Diagram formatters for lineage graphs (Mermaid and DOT/Graphviz)."""

import re
from typing import Set

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
