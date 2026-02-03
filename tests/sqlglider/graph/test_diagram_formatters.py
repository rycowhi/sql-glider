"""Tests for diagram formatters (Mermaid, DOT, and Plotly)."""

import json

import pytest

from sqlglider.graph.diagram_formatters import (
    LEAF_FILL,
    QUERIED_FILL,
    ROOT_FILL,
    DotFormatter,
    MermaidFormatter,
    MermaidMarkdownFormatter,
    PlotlyFormatter,
    _collect_query_edges,
    _collect_query_nodes,
    _compute_layered_layout,
    _quote_dot_id,
    _sanitize_mermaid_id,
)
from sqlglider.graph.models import (
    GraphEdge,
    GraphNode,
    LineageGraph,
    LineageNode,
    LineagePath,
)
from sqlglider.graph.query import LineageQueryResult

# --- Fixtures ---


@pytest.fixture
def empty_graph():
    """Empty graph with no nodes or edges."""
    return LineageGraph(nodes=[], edges=[])


@pytest.fixture
def linear_graph():
    """Linear graph: a.col -> b.col -> c.col."""
    return LineageGraph(
        nodes=[
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
        ],
        edges=[
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ],
    )


@pytest.fixture
def diamond_graph():
    """Diamond graph: a.col -> b.col, a.col -> c.col, b.col -> d.col, c.col -> d.col."""
    return LineageGraph(
        nodes=[
            GraphNode.from_identifier("a.col", "/path/q.sql", 0),
            GraphNode.from_identifier("b.col", "/path/q.sql", 0),
            GraphNode.from_identifier("c.col", "/path/q.sql", 0),
            GraphNode.from_identifier("d.col", "/path/q.sql", 0),
        ],
        edges=[
            GraphEdge(
                source_node="a.col",
                target_node="b.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="a.col",
                target_node="c.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="b.col",
                target_node="d.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
            GraphEdge(
                source_node="c.col",
                target_node="d.col",
                file_path="/path/q.sql",
                query_index=0,
            ),
        ],
    )


@pytest.fixture
def empty_query_result():
    """Query result with no related columns."""
    return LineageQueryResult(
        query_column="isolated.col",
        direction="upstream",
        related_columns=[],
    )


@pytest.fixture
def upstream_query_result():
    """Upstream query result: source.col -> mid.col -> target.col."""
    return LineageQueryResult(
        query_column="target.col",
        direction="upstream",
        related_columns=[
            LineageNode.from_graph_node(
                GraphNode.from_identifier("source.col", "/path/q.sql", 0),
                hops=2,
                output_column="target.col",
                is_root=True,
                is_leaf=False,
                paths=[
                    LineagePath(nodes=["source.col", "mid.col", "target.col"]),
                ],
            ),
            LineageNode.from_graph_node(
                GraphNode.from_identifier("mid.col", "/path/q.sql", 0),
                hops=1,
                output_column="target.col",
                is_root=False,
                is_leaf=False,
                paths=[
                    LineagePath(nodes=["mid.col", "target.col"]),
                ],
            ),
        ],
    )


@pytest.fixture
def downstream_query_result():
    """Downstream query result with a leaf node."""
    return LineageQueryResult(
        query_column="source.col",
        direction="downstream",
        related_columns=[
            LineageNode.from_graph_node(
                GraphNode.from_identifier("target.col", "/path/q.sql", 0),
                hops=1,
                output_column="source.col",
                is_root=False,
                is_leaf=True,
                paths=[
                    LineagePath(nodes=["source.col", "target.col"]),
                ],
            ),
        ],
    )


# --- Helper function tests ---


class TestSanitizeMermaidId:
    """Tests for _sanitize_mermaid_id."""

    def test_simple_identifier(self):
        assert _sanitize_mermaid_id("table_col") == "table_col"

    def test_dots_replaced(self):
        assert _sanitize_mermaid_id("schema.table.col") == "schema_table_col"

    def test_special_chars_replaced(self):
        assert _sanitize_mermaid_id("a-b/c") == "a_b_c"


class TestQuoteDotId:
    """Tests for _quote_dot_id."""

    def test_simple(self):
        assert _quote_dot_id("table.col") == '"table.col"'

    def test_escapes_quotes(self):
        assert _quote_dot_id('a"b') == '"a\\"b"'

    def test_escapes_backslashes(self):
        assert _quote_dot_id("a\\b") == '"a\\\\b"'


class TestCollectQueryEdges:
    """Tests for _collect_query_edges."""

    def test_extracts_edges_from_paths(self, upstream_query_result):
        edges = _collect_query_edges(upstream_query_result)
        assert ("source.col", "mid.col") in edges
        assert ("mid.col", "target.col") in edges

    def test_empty_result(self, empty_query_result):
        edges = _collect_query_edges(empty_query_result)
        assert edges == set()


class TestCollectQueryNodes:
    """Tests for _collect_query_nodes."""

    def test_extracts_nodes_from_paths(self, upstream_query_result):
        nodes = _collect_query_nodes(upstream_query_result)
        assert nodes == {"source.col", "mid.col", "target.col"}

    def test_empty_result_includes_queried_column(self, empty_query_result):
        nodes = _collect_query_nodes(empty_query_result)
        assert nodes == {"isolated.col"}


# --- MermaidFormatter tests ---


class TestMermaidFormatterFullGraph:
    """Tests for MermaidFormatter.format_full_graph."""

    def test_empty_graph(self, empty_graph):
        result = MermaidFormatter.format_full_graph(empty_graph)
        assert result == "flowchart TD"

    def test_linear_graph_has_nodes(self, linear_graph):
        result = MermaidFormatter.format_full_graph(linear_graph)
        assert 'a_col["a.col"]' in result
        assert 'b_col["b.col"]' in result
        assert 'c_col["c.col"]' in result

    def test_linear_graph_has_edges(self, linear_graph):
        result = MermaidFormatter.format_full_graph(linear_graph)
        assert "a_col --> b_col" in result
        assert "b_col --> c_col" in result

    def test_diamond_graph_edges(self, diamond_graph):
        result = MermaidFormatter.format_full_graph(diamond_graph)
        assert "a_col --> b_col" in result
        assert "a_col --> c_col" in result
        assert "b_col --> d_col" in result
        assert "c_col --> d_col" in result

    def test_starts_with_flowchart(self, linear_graph):
        result = MermaidFormatter.format_full_graph(linear_graph)
        assert result.startswith("flowchart TD")


class TestMermaidFormatterQueryResult:
    """Tests for MermaidFormatter.format_query_result."""

    def test_empty_result_shows_queried_node(self, empty_query_result):
        result = MermaidFormatter.format_query_result(empty_query_result)
        assert 'isolated_col["isolated.col"]' in result

    def test_upstream_has_edges(self, upstream_query_result):
        result = MermaidFormatter.format_query_result(upstream_query_result)
        assert "source_col --> mid_col" in result
        assert "mid_col --> target_col" in result

    def test_queried_node_styled_amber(self, upstream_query_result):
        result = MermaidFormatter.format_query_result(upstream_query_result)
        assert f"style target_col fill:{QUERIED_FILL}" in result

    def test_root_node_styled_teal(self, upstream_query_result):
        result = MermaidFormatter.format_query_result(upstream_query_result)
        assert f"style source_col fill:{ROOT_FILL}" in result

    def test_leaf_node_styled_violet(self, downstream_query_result):
        result = MermaidFormatter.format_query_result(downstream_query_result)
        assert f"style target_col fill:{LEAF_FILL}" in result

    def test_legend_present(self, upstream_query_result):
        result = MermaidFormatter.format_query_result(upstream_query_result)
        assert "subgraph Legend" in result
        assert "Queried Column" in result
        assert "Root (no upstream)" in result
        assert "Leaf (no downstream)" in result

    def test_legend_absent_for_empty_result(self, empty_query_result):
        result = MermaidFormatter.format_query_result(empty_query_result)
        assert "Legend" not in result


# --- DotFormatter tests ---


class TestDotFormatterFullGraph:
    """Tests for DotFormatter.format_full_graph."""

    def test_empty_graph(self, empty_graph):
        result = DotFormatter.format_full_graph(empty_graph)
        assert "digraph lineage {" in result
        assert result.strip().endswith("}")

    def test_linear_graph_has_nodes(self, linear_graph):
        result = DotFormatter.format_full_graph(linear_graph)
        assert '"a.col";' in result
        assert '"b.col";' in result

    def test_linear_graph_has_edges(self, linear_graph):
        result = DotFormatter.format_full_graph(linear_graph)
        assert '"a.col" -> "b.col";' in result
        assert '"b.col" -> "c.col";' in result

    def test_has_rankdir(self, linear_graph):
        result = DotFormatter.format_full_graph(linear_graph)
        assert "rankdir=LR" in result

    def test_diamond_graph_edges(self, diamond_graph):
        result = DotFormatter.format_full_graph(diamond_graph)
        assert '"a.col" -> "b.col";' in result
        assert '"a.col" -> "c.col";' in result
        assert '"b.col" -> "d.col";' in result
        assert '"c.col" -> "d.col";' in result


class TestDotFormatterQueryResult:
    """Tests for DotFormatter.format_query_result."""

    def test_empty_result_shows_queried_node(self, empty_query_result):
        result = DotFormatter.format_query_result(empty_query_result)
        assert '"isolated.col"' in result
        assert "fillcolor" in result

    def test_upstream_has_edges(self, upstream_query_result):
        result = DotFormatter.format_query_result(upstream_query_result)
        assert '"source.col" -> "mid.col";' in result
        assert '"mid.col" -> "target.col";' in result

    def test_queried_node_styled(self, upstream_query_result):
        result = DotFormatter.format_query_result(upstream_query_result)
        assert (
            f'"target.col" [style="rounded,filled", fillcolor="{QUERIED_FILL}"]'
            in result
        )

    def test_root_node_styled(self, upstream_query_result):
        result = DotFormatter.format_query_result(upstream_query_result)
        assert (
            f'"source.col" [style="rounded,filled", fillcolor="{ROOT_FILL}"]' in result
        )

    def test_leaf_node_styled(self, downstream_query_result):
        result = DotFormatter.format_query_result(downstream_query_result)
        assert (
            f'"target.col" [style="rounded,filled", fillcolor="{LEAF_FILL}"]' in result
        )

    def test_valid_digraph_structure(self, upstream_query_result):
        result = DotFormatter.format_query_result(upstream_query_result)
        assert result.startswith("digraph lineage {")
        assert result.strip().endswith("}")

    def test_legend_present(self, upstream_query_result):
        result = DotFormatter.format_query_result(upstream_query_result)
        assert "cluster_legend" in result
        assert "Queried Column" in result
        assert "Root (no upstream)" in result
        assert "Leaf (no downstream)" in result

    def test_legend_absent_for_empty_result(self, empty_query_result):
        result = DotFormatter.format_query_result(empty_query_result)
        assert "cluster_legend" not in result


# --- MermaidMarkdownFormatter tests ---


class TestMermaidMarkdownFormatterFullGraph:
    """Tests for MermaidMarkdownFormatter.format_full_graph."""

    def test_wraps_in_code_fence(self, linear_graph):
        result = MermaidMarkdownFormatter.format_full_graph(linear_graph)
        assert result.startswith("```mermaid\n")
        assert result.endswith("\n```")

    def test_contains_mermaid_content(self, linear_graph):
        result = MermaidMarkdownFormatter.format_full_graph(linear_graph)
        assert "flowchart TD" in result
        assert "a_col --> b_col" in result

    def test_empty_graph(self, empty_graph):
        result = MermaidMarkdownFormatter.format_full_graph(empty_graph)
        assert result == "```mermaid\nflowchart TD\n```"

    def test_matches_mermaid_content(self, diamond_graph):
        mermaid = MermaidFormatter.format_full_graph(diamond_graph)
        markdown = MermaidMarkdownFormatter.format_full_graph(diamond_graph)
        assert markdown == f"```mermaid\n{mermaid}\n```"


class TestMermaidMarkdownFormatterQueryResult:
    """Tests for MermaidMarkdownFormatter.format_query_result."""

    def test_wraps_in_code_fence(self, upstream_query_result):
        result = MermaidMarkdownFormatter.format_query_result(upstream_query_result)
        assert result.startswith("```mermaid\n")
        assert result.endswith("\n```")

    def test_contains_styling(self, upstream_query_result):
        result = MermaidMarkdownFormatter.format_query_result(upstream_query_result)
        assert f"style target_col fill:{QUERIED_FILL}" in result

    def test_empty_result(self, empty_query_result):
        result = MermaidMarkdownFormatter.format_query_result(empty_query_result)
        assert result.startswith("```mermaid\n")
        assert result.endswith("\n```")
        assert "isolated_col" in result

    def test_matches_mermaid_content(self, upstream_query_result):
        mermaid = MermaidFormatter.format_query_result(upstream_query_result)
        markdown = MermaidMarkdownFormatter.format_query_result(upstream_query_result)
        assert markdown == f"```mermaid\n{mermaid}\n```"


# --- Layout algorithm tests ---


class TestComputeLayeredLayout:
    """Tests for _compute_layered_layout helper function."""

    def test_empty_nodes(self):
        positions = _compute_layered_layout([], [])
        assert positions == {}

    def test_single_node(self):
        positions = _compute_layered_layout(["a"], [])
        assert "a" in positions
        assert positions["a"] == (0, 0.0)

    def test_linear_chain(self):
        nodes = ["a", "b", "c"]
        edges = [("a", "b"), ("b", "c")]
        positions = _compute_layered_layout(nodes, edges)

        # a should be at layer 0, b at layer 1, c at layer 2
        assert positions["a"][0] < positions["b"][0]
        assert positions["b"][0] < positions["c"][0]

    def test_diamond_layout(self):
        nodes = ["a", "b", "c", "d"]
        edges = [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")]
        positions = _compute_layered_layout(nodes, edges)

        # a at layer 0, b and c at layer 1, d at layer 2
        assert positions["a"][0] < positions["b"][0]
        assert positions["a"][0] < positions["c"][0]
        assert positions["b"][0] < positions["d"][0]
        assert positions["c"][0] < positions["d"][0]
        # b and c should be at the same x position
        assert positions["b"][0] == positions["c"][0]

    def test_multiple_roots(self):
        nodes = ["a", "b", "c"]
        edges = [("a", "c"), ("b", "c")]
        positions = _compute_layered_layout(nodes, edges)

        # a and b should be at layer 0, c at layer 1
        assert positions["a"][0] == positions["b"][0]
        assert positions["a"][0] < positions["c"][0]


# --- PlotlyFormatter tests ---


@pytest.fixture
def plotly_available():
    """Check if plotly is available for testing."""
    try:
        import plotly  # noqa: F401

        return True
    except ImportError:
        return False


class TestPlotlyFormatterFullGraph:
    """Tests for PlotlyFormatter.format_full_graph."""

    def test_empty_graph(self, empty_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(empty_graph)
        figure = json.loads(result)

        assert "data" in figure
        assert "layout" in figure
        assert figure["data"] == []
        assert figure["layout"]["title"]["text"] == "Lineage Graph"

    def test_linear_graph_has_valid_json(self, linear_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(linear_graph)
        figure = json.loads(result)

        assert "data" in figure
        assert "layout" in figure
        assert len(figure["data"]) > 0

    def test_linear_graph_has_edge_traces(self, linear_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(linear_graph)
        figure = json.loads(result)

        # Should have edge traces (lines) plus one node trace
        edge_traces = [t for t in figure["data"] if t.get("mode") == "lines"]
        assert len(edge_traces) == 2  # Two edges in linear graph

    def test_linear_graph_has_node_trace(self, linear_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(linear_graph)
        figure = json.loads(result)

        # Find node trace (markers+text mode)
        node_traces = [t for t in figure["data"] if t.get("mode") == "markers+text"]
        assert len(node_traces) == 1

        # Should have 3 nodes
        node_trace = node_traces[0]
        assert len(node_trace["x"]) == 3
        assert len(node_trace["y"]) == 3
        assert len(node_trace["text"]) == 3

    def test_diamond_graph_has_correct_edges(self, diamond_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(diamond_graph)
        figure = json.loads(result)

        edge_traces = [t for t in figure["data"] if t.get("mode") == "lines"]
        assert len(edge_traces) == 4  # Four edges in diamond

    def test_layout_has_hidden_axes(self, linear_graph, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_full_graph(linear_graph)
        figure = json.loads(result)

        assert figure["layout"]["xaxis"]["visible"] is False
        assert figure["layout"]["yaxis"]["visible"] is False


class TestPlotlyFormatterQueryResult:
    """Tests for PlotlyFormatter.format_query_result."""

    def test_empty_result_has_valid_json(self, empty_query_result, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(empty_query_result)
        figure = json.loads(result)

        assert "data" in figure
        assert "layout" in figure

    def test_upstream_has_edges(self, upstream_query_result, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(upstream_query_result)
        figure = json.loads(result)

        edge_traces = [t for t in figure["data"] if t.get("mode") == "lines"]
        assert len(edge_traces) == 2  # source->mid, mid->target

    def test_queried_node_has_amber_color(
        self, upstream_query_result, plotly_available
    ):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(upstream_query_result)
        figure = json.loads(result)

        node_traces = [t for t in figure["data"] if t.get("mode") == "markers+text"]
        assert len(node_traces) == 1

        node_trace = node_traces[0]
        colors = node_trace["marker"]["color"]
        # target.col is the queried column and should be amber
        assert QUERIED_FILL in colors

    def test_root_node_has_teal_color(self, upstream_query_result, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(upstream_query_result)
        figure = json.loads(result)

        node_traces = [t for t in figure["data"] if t.get("mode") == "markers+text"]
        node_trace = node_traces[0]
        colors = node_trace["marker"]["color"]
        # source.col is a root and should be teal
        assert ROOT_FILL in colors

    def test_leaf_node_has_violet_color(
        self, downstream_query_result, plotly_available
    ):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(downstream_query_result)
        figure = json.loads(result)

        node_traces = [t for t in figure["data"] if t.get("mode") == "markers+text"]
        node_trace = node_traces[0]
        colors = node_trace["marker"]["color"]
        # target.col is a leaf and should be violet
        assert LEAF_FILL in colors

    def test_has_legend_traces(self, upstream_query_result, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(upstream_query_result)
        figure = json.loads(result)

        legend_traces = [t for t in figure["data"] if t.get("showlegend") is True]
        assert len(legend_traces) == 3  # Queried, Root, Leaf

        legend_names = [t["name"] for t in legend_traces]
        assert "Queried Column" in legend_names
        assert "Root (no upstream)" in legend_names
        assert "Leaf (no downstream)" in legend_names

    def test_title_includes_direction_and_column(
        self, upstream_query_result, plotly_available
    ):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(upstream_query_result)
        figure = json.loads(result)

        title = figure["layout"]["title"]["text"]
        assert "Upstream" in title
        assert "target.col" in title

    def test_downstream_title(self, downstream_query_result, plotly_available):
        if not plotly_available:
            pytest.skip("plotly not installed")

        result = PlotlyFormatter.format_query_result(downstream_query_result)
        figure = json.loads(result)

        title = figure["layout"]["title"]["text"]
        assert "Downstream" in title
        assert "source.col" in title


class TestPlotlyFormatterDependency:
    """Tests for PlotlyFormatter dependency handling."""

    def test_raises_import_error_without_plotly(
        self, empty_graph, monkeypatch, plotly_available
    ):
        """Test that ImportError is raised when plotly is not available."""
        if plotly_available:
            # Simulate plotly not being installed
            import builtins

            original_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "plotly":
                    raise ImportError("No module named 'plotly'")
                return original_import(name, *args, **kwargs)

            monkeypatch.setattr(builtins, "__import__", mock_import)

            with pytest.raises(ImportError) as exc_info:
                PlotlyFormatter.format_full_graph(empty_graph)

            assert "plotly" in str(exc_info.value).lower()
        else:
            # Plotly not installed, should raise ImportError naturally
            with pytest.raises(ImportError) as exc_info:
                PlotlyFormatter.format_full_graph(empty_graph)

            assert "plotly" in str(exc_info.value).lower()
