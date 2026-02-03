# Plan: Mermaid & DOT Diagram Output for Graph Commands

**Status:** Completed

## Overview

Add Mermaid and DOT (Graphviz) diagram output formats to the graph subsystem, enabling visual lineage diagrams from both query results and full graphs. Also add a `mermaid-markdown` format that wraps Mermaid output in markdown code fences for quick rendering in GitHub, Notion, etc.

## Design Decisions and Rationale

- **Mermaid chosen as primary diagram format** — text-based, renders natively in GitHub Markdown, GitLab, and many documentation tools. Most portable option for CLI output.
- **DOT (Graphviz) as secondary format** — the classic graph description language with more powerful layout control. Good for users who want to pipe into `dot` to generate SVG/PNG.
- **D2, PlantUML, and ASCII art were considered but not chosen** — D2 has a smaller ecosystem, PlantUML requires a heavier toolchain, ASCII art is too limited for complex graphs.
- **Mermaid-markdown format added** — wraps raw Mermaid in `` ```mermaid `` fences for direct paste into markdown files.
- **Shared formatter module** — `diagram_formatters.py` serves both `graph query` and `graph visualize` commands, avoiding duplication.
- **Static method pattern** — follows the existing formatter pattern used by `lineage/formatters.py` and `dissection/formatters.py`.
- **Muted jewel tone color palette** — amber/teal/violet chosen over red/green/blue for light/dark mode compatibility and color accessibility.
- **Legend subgraph** — included in query result diagrams (both Mermaid and DOT) to explain node color meanings.

## Implementation Steps

- [x] Create `src/sqlglider/graph/diagram_formatters.py` with `MermaidFormatter`, `MermaidMarkdownFormatter`, and `DotFormatter` classes
- [x] Add shared helper functions: `_sanitize_mermaid_id()`, `_quote_dot_id()`, `_collect_query_edges()`, `_collect_query_nodes()`
- [x] Define color palette as module-level constants (`QUERIED_FILL`, `ROOT_FILL`, `LEAF_FILL`, etc.)
- [x] Implement `format_full_graph()` for both Mermaid and DOT (renders entire `LineageGraph`)
- [x] Implement `format_query_result()` for both Mermaid and DOT (renders `LineageQueryResult` with styling and legend)
- [x] Implement `MermaidMarkdownFormatter` wrapping Mermaid output in markdown code fences
- [x] Add `graph visualize` CLI command with `-f mermaid|mermaid-markdown|dot` and `-o` file output
- [x] Extend `graph query` CLI command to accept `mermaid`, `mermaid-markdown`, and `dot` as output formats
- [x] Create `tests/sqlglider/graph/test_diagram_formatters.py` with 43 tests
- [x] Update documentation (ARCHITECTURE.md, CLAUDE.md, README.md)
- [x] Run linter, type checker, and full test suite

## Files Created or Modified

### Created
- `src/sqlglider/graph/diagram_formatters.py` — Formatter classes and helpers
- `tests/sqlglider/graph/test_diagram_formatters.py` — 43 unit tests

### Modified
- `src/sqlglider/cli.py` — Added `graph visualize` command; extended `graph query` format options
- `ARCHITECTURE.md` — Added `diagram_formatters.py` to project structure and CLI examples
- `CLAUDE.md` — Added CLI examples for diagram formats and visualize command
- `README.md` — Added user-facing examples for diagram output

## Testing Strategy

- Unit tests for all three formatter classes (`MermaidFormatter`, `MermaidMarkdownFormatter`, `DotFormatter`)
- Tests for both `format_full_graph()` and `format_query_result()` methods
- Coverage of empty graphs, linear graphs, diamond graphs, and empty query results
- Validation of node styling (queried/root/leaf colors), edge rendering, and legend presence/absence
- Helper function tests for ID sanitization, quoting, and path extraction
- MermaidMarkdown tests verify exact wrapping and content match with raw Mermaid output
- Full test suite passes (740+ tests), coverage at 82%+ above 80% threshold

## Implementation Notes

- Colors were initially red/green/blue (`#ff9999`/`#99ff99`/`#9999ff`) but changed to muted jewel tones (amber/teal/violet) after testing showed poor visibility in dark mode
- Legend is only rendered for query results with actual related columns, not for empty results or full graph visualizations
- DOT legend uses `cluster_legend` subgraph with `style=dashed` and invisible edges for vertical stacking
- Mermaid legend uses `subgraph Legend` with styled placeholder nodes
- The `graph visualize` command defaults to `mermaid` format (not `text`) since it's diagram-only
