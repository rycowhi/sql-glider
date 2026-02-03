#!/usr/bin/env python3
"""Simple Dash app to view SQL Glider Plotly lineage output.

Usage:
    # graph visualize has -o flag for file output
    uv run sqlglider graph visualize graph.json -f plotly -o lineage.json
    uv run python examples/plotly_viewer.py lineage.json

    # graph query needs shell redirect - use PowerShell on Windows for proper encoding
    uv run sqlglider graph query graph.json --upstream orders.total -f plotly | Out-File -Encoding utf8 lineage.json
    uv run python examples/plotly_viewer.py lineage.json

    # On Unix/macOS:
    sqlglider graph query graph.json --upstream orders.total -f plotly > lineage.json
    python examples/plotly_viewer.py lineage.json
"""

import json
import sys
from pathlib import Path

try:
    import dash
    from dash import dcc, html
except ImportError:
    print("Error: Dash is required. Install with: pip install dash", file=sys.stderr)
    sys.exit(1)


def load_figure(source: str | Path | None = None) -> dict:
    """Load Plotly figure from file or stdin."""
    if source is None or source == "-":
        # Read from stdin
        if sys.stdin.isatty():
            print("Usage: python plotly_viewer.py <lineage.json>", file=sys.stderr)
            print(
                "   or: sqlglider graph visualize graph.json -f plotly | python plotly_viewer.py",
                file=sys.stderr,
            )
            sys.exit(1)
        content = sys.stdin.read()
        if not content.strip():
            print("Error: No input received from stdin", file=sys.stderr)
            print("Note: On Windows, piping may not work reliably.", file=sys.stderr)
            print(
                "Try: sqlglider graph query ... -f plotly -o output.json",
                file=sys.stderr,
            )
            print("Then: python plotly_viewer.py output.json", file=sys.stderr)
            sys.exit(1)
    else:
        path = Path(source)
        if not path.exists():
            print(f"Error: File not found: {path}", file=sys.stderr)
            sys.exit(1)
        # Try multiple encodings - Windows redirect can create UTF-16 files
        for encoding in ["utf-8", "utf-16", "utf-8-sig"]:
            try:
                content = path.read_text(encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            print(
                "Error: Could not decode file with UTF-8 or UTF-16 encoding",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}", file=sys.stderr)
        if len(content) < 200:
            print(f"Content received: {content!r}", file=sys.stderr)
        else:
            print(f"Content starts with: {content[:200]!r}...", file=sys.stderr)
        sys.exit(1)


def create_app(figure: dict) -> dash.Dash:
    """Create Dash app with the lineage graph."""
    app = dash.Dash(__name__)

    title = figure.get("layout", {}).get("title", {}).get("text", "Lineage Graph")

    app.layout = html.Div(
        [
            html.H1(title, style={"textAlign": "center", "fontFamily": "sans-serif"}),
            dcc.Graph(
                id="lineage-graph",
                figure=figure,
                style={"height": "85vh"},
                config={
                    "displayModeBar": True,
                    "scrollZoom": True,
                    "modeBarButtonsToAdd": ["select2d", "lasso2d"],
                },
            ),
        ],
        style={"padding": "20px"},
    )

    return app


def main():
    source = sys.argv[1] if len(sys.argv) > 1 else None
    figure = load_figure(source)
    app = create_app(figure)

    print("Starting Dash server at http://127.0.0.1:8050")
    print("Press Ctrl+C to stop")
    app.run(debug=True, host="127.0.0.1", port=8050)


if __name__ == "__main__":
    main()
