"""Graph builder for constructing lineage graphs from SQL files."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Literal, Optional, Set

import rustworkx as rx
from rich.console import Console

from sqlglider.graph.models import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    LineageGraph,
    Manifest,
)
from sqlglider.lineage.analyzer import LineageAnalyzer
from sqlglider.utils.file_utils import read_sql_file

console = Console(stderr=True)

# Type alias for SQL preprocessor functions
SqlPreprocessor = Callable[[str, Path], str]


class GraphBuilder:
    """Build lineage graphs from SQL files using rustworkx."""

    def __init__(
        self,
        node_format: Literal["qualified", "structured"] = "qualified",
        dialect: str = "spark",
        sql_preprocessor: Optional[SqlPreprocessor] = None,
    ):
        """
        Initialize the graph builder.

        Args:
            node_format: Format for node identifiers ("qualified" or "structured")
            dialect: Default SQL dialect (used when not specified per-file)
            sql_preprocessor: Optional function to preprocess SQL before analysis.
                             Takes (sql: str, file_path: Path) and returns processed SQL.
                             Useful for templating (e.g., Jinja2 rendering).
        """
        self.node_format = node_format
        self.dialect = dialect
        self.sql_preprocessor = sql_preprocessor
        self.graph: rx.PyDiGraph = rx.PyDiGraph()
        self._node_index_map: Dict[str, int] = {}  # identifier -> rustworkx node index
        self._source_files: Set[str] = set()
        self._edge_set: Set[tuple] = set()  # (source, target) for dedup
        self._skipped_files: List[tuple[str, str]] = []  # (file_path, reason)

    def add_file(
        self,
        file_path: Path,
        dialect: Optional[str] = None,
    ) -> "GraphBuilder":
        """
        Add lineage from a single SQL file to the graph.

        Args:
            file_path: Path to SQL file
            dialect: SQL dialect (uses builder default if not specified)

        Returns:
            self for method chaining

        Raises:
            FileNotFoundError: If file doesn't exist
            ParseError: If SQL cannot be parsed
        """
        file_dialect = dialect or self.dialect
        file_path_str = str(file_path.resolve())

        try:
            sql_content = read_sql_file(file_path)

            # Apply SQL preprocessor if configured (e.g., for templating)
            if self.sql_preprocessor:
                sql_content = self.sql_preprocessor(sql_content, file_path)

            analyzer = LineageAnalyzer(sql_content, dialect=file_dialect)
            results = analyzer.analyze_queries(level="column")

            # Print warnings for any skipped queries within the file
            for skipped in analyzer.skipped_queries:
                console.print(
                    f"[yellow]Warning:[/yellow] Skipping query {skipped.query_index} "
                    f"in {file_path.name} ({skipped.statement_type}): {skipped.reason}"
                )

            self._source_files.add(file_path_str)

            for result in results:
                query_index = result.metadata.query_index

                for item in result.lineage_items:
                    if not item.source_name:  # Skip empty sources
                        continue

                    # Add/get nodes
                    source_node_idx = self._ensure_node(
                        item.source_name,
                        file_path_str,
                        query_index,
                    )
                    target_node_idx = self._ensure_node(
                        item.output_name,
                        file_path_str,
                        query_index,
                    )

                    # Add edge (source contributes_to target) - deduplicate
                    edge_key = (item.source_name, item.output_name)
                    if edge_key not in self._edge_set:
                        edge = GraphEdge(
                            source_node=item.source_name,
                            target_node=item.output_name,
                            file_path=file_path_str,
                            query_index=query_index,
                        )
                        self.graph.add_edge(
                            source_node_idx, target_node_idx, edge.model_dump()
                        )
                        self._edge_set.add(edge_key)

        except ValueError as e:
            # Skip files that fail completely (all statements unsupported)
            error_msg = str(e)
            self._skipped_files.append((file_path_str, error_msg))
            console.print(
                f"[yellow]Warning:[/yellow] Skipping {file_path.name}: {error_msg}"
            )

        return self

    def add_directory(
        self,
        dir_path: Path,
        recursive: bool = False,
        glob_pattern: str = "*.sql",
        dialect: Optional[str] = None,
    ) -> "GraphBuilder":
        """
        Add lineage from all SQL files in a directory.

        Args:
            dir_path: Path to directory
            recursive: Whether to search recursively
            glob_pattern: Glob pattern for SQL files
            dialect: SQL dialect (uses builder default if not specified)

        Returns:
            self for method chaining

        Raises:
            ValueError: If path is not a directory
        """
        if not dir_path.is_dir():
            raise ValueError(f"Not a directory: {dir_path}")

        if recursive:
            pattern = f"**/{glob_pattern}"
        else:
            pattern = glob_pattern

        for sql_file in sorted(dir_path.glob(pattern)):
            if sql_file.is_file():
                self.add_file(sql_file, dialect)

        return self

    def add_manifest(
        self,
        manifest_path: Path,
        dialect: Optional[str] = None,
    ) -> "GraphBuilder":
        """
        Add lineage from files specified in a manifest CSV.

        Args:
            manifest_path: Path to manifest CSV file
            dialect: Default SQL dialect (overridden by manifest entries)

        Returns:
            self for method chaining

        Raises:
            FileNotFoundError: If manifest or referenced files don't exist
            ValueError: If manifest format is invalid
        """
        manifest = Manifest.from_csv(manifest_path)
        base_dir = manifest_path.parent

        for entry in manifest.entries:
            # Resolve file path relative to manifest location
            file_path = Path(entry.file_path)
            if not file_path.is_absolute():
                file_path = (base_dir / entry.file_path).resolve()

            # Use entry dialect, then CLI dialect, then builder default
            entry_dialect = entry.dialect or dialect or self.dialect
            self.add_file(file_path, entry_dialect)

        return self

    def add_files(
        self,
        file_paths: List[Path],
        dialect: Optional[str] = None,
    ) -> "GraphBuilder":
        """
        Add lineage from multiple SQL files.

        Args:
            file_paths: List of paths to SQL files
            dialect: SQL dialect (uses builder default if not specified)

        Returns:
            self for method chaining
        """
        for file_path in file_paths:
            self.add_file(file_path, dialect)
        return self

    def _ensure_node(
        self,
        identifier: str,
        file_path: str,
        query_index: int,
    ) -> int:
        """
        Ensure a node exists in the graph, creating it if necessary.

        Args:
            identifier: Node identifier (e.g., "table.column")
            file_path: Source file path
            query_index: Query index within file

        Returns:
            rustworkx node index
        """
        if identifier in self._node_index_map:
            return self._node_index_map[identifier]

        node = GraphNode.from_identifier(
            identifier=identifier,
            file_path=file_path,
            query_index=query_index,
        )

        node_idx = self.graph.add_node(node.model_dump())
        self._node_index_map[identifier] = node_idx
        return node_idx

    def build(self) -> LineageGraph:
        """
        Build and return the final LineageGraph.

        Returns:
            LineageGraph with metadata, nodes, and edges
        """
        nodes = []
        for idx in self.graph.node_indices():
            node_data = self.graph[idx]
            nodes.append(GraphNode(**node_data))

        edges = []
        for edge_idx in self.graph.edge_indices():
            edge_data = self.graph.get_edge_data_by_index(edge_idx)
            edges.append(GraphEdge(**edge_data))

        metadata = GraphMetadata(
            node_format=self.node_format,
            default_dialect=self.dialect,
            created_at=datetime.now(timezone.utc).isoformat(),
            source_files=sorted(self._source_files),
            total_nodes=len(nodes),
            total_edges=len(edges),
        )

        # Print summary of skipped files if any
        if self._skipped_files:
            console.print(
                f"\n[yellow]Summary:[/yellow] Skipped {len(self._skipped_files)} "
                f"file(s) that could not be analyzed for lineage."
            )

        return LineageGraph(
            metadata=metadata,
            nodes=nodes,
            edges=edges,
        )

    @property
    def rustworkx_graph(self) -> rx.PyDiGraph:
        """Get the underlying rustworkx graph for direct operations."""
        return self.graph

    @property
    def node_index_map(self) -> Dict[str, int]:
        """Get mapping from node identifiers to rustworkx indices."""
        return self._node_index_map.copy()

    @property
    def skipped_files(self) -> List[tuple[str, str]]:
        """Get list of files that were skipped during graph building."""
        return self._skipped_files.copy()
