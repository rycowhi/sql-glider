"""Pydantic models for graph-based lineage representation."""

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field

from sqlglider.global_models import NodeFormat


class GraphNode(BaseModel):
    """Represents a node in the lineage graph (a column)."""

    identifier: str = Field(
        ..., description="Unique node identifier (fully-qualified column name)"
    )
    file_path: str = Field(
        ..., description="Source SQL file path where first encountered"
    )
    query_index: int = Field(..., description="Index of query within the file")

    # Structured fields for flexible querying (always populated from identifier)
    schema_name: Optional[str] = Field(None, description="Schema name (if present)")
    table: Optional[str] = Field(None, description="Table name")
    column: Optional[str] = Field(None, description="Column name")

    @classmethod
    def from_identifier(
        cls,
        identifier: str,
        file_path: str,
        query_index: int,
    ) -> "GraphNode":
        """
        Create a GraphNode from a column identifier.

        Parses the identifier into schema, table, and column components.

        Args:
            identifier: Fully-qualified column name (e.g., "schema.table.column" or "table.column")
            file_path: Source SQL file path
            query_index: Query index within the file

        Returns:
            GraphNode with parsed components
        """
        parts = identifier.split(".")

        if len(parts) >= 3:
            schema_name = parts[0]
            table = parts[1]
            column = ".".join(parts[2:])  # Handle columns with dots
        elif len(parts) == 2:
            schema_name = None
            table = parts[0]
            column = parts[1]
        else:
            schema_name = None
            table = None
            column = identifier

        return cls(
            identifier=identifier,
            file_path=file_path,
            query_index=query_index,
            schema_name=schema_name,
            table=table,
            column=column,
        )


class GraphEdge(BaseModel):
    """Represents an edge in the lineage graph (contributes_to relationship)."""

    source_node: str = Field(
        ..., description="Source node identifier (contributes from)"
    )
    target_node: str = Field(..., description="Target node identifier (contributes to)")
    file_path: str = Field(
        ..., description="Source SQL file where relationship is defined"
    )
    query_index: int = Field(..., description="Index of query within the file")


class ManifestEntry(BaseModel):
    """Represents a single entry in a manifest file."""

    file_path: str = Field(..., description="Path to SQL file")
    dialect: Optional[str] = Field(
        None, description="SQL dialect (optional, uses default if empty)"
    )


class Manifest(BaseModel):
    """Represents a manifest file with SQL file paths and optional dialects."""

    entries: List[ManifestEntry] = Field(default_factory=list)

    @classmethod
    def from_csv(cls, csv_path: Path) -> "Manifest":
        """
        Load manifest from CSV file.

        Expected CSV format:
        ```
        file_path,dialect
        queries/orders.sql,spark
        queries/customers.sql,postgres
        queries/legacy.sql,
        ```

        Args:
            csv_path: Path to manifest CSV file

        Returns:
            Manifest with loaded entries

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV is missing required 'file_path' column
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {csv_path}")

        entries = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate required column
            if reader.fieldnames is None or "file_path" not in reader.fieldnames:
                raise ValueError("Manifest CSV must have a 'file_path' column")

            for row in reader:
                file_path = row["file_path"].strip()
                if not file_path:
                    continue  # Skip empty rows

                dialect = row.get("dialect", "").strip() or None
                entries.append(ManifestEntry(file_path=file_path, dialect=dialect))

        return cls(entries=entries)


class LineagePath(BaseModel):
    """A single lineage path from a node to the queried column."""

    nodes: List[str] = Field(
        ..., description="Ordered list of node identifiers in the path"
    )

    @property
    def hops(self) -> int:
        """Number of hops in the path (edges traversed)."""
        return len(self.nodes) - 1 if len(self.nodes) > 1 else 0

    def to_arrow_string(self) -> str:
        """Format path as arrow-separated string for display."""
        return " -> ".join(self.nodes)


class LineageNode(BaseModel):
    """
    A node in lineage query results with additional context.

    Extends GraphNode fields with query-specific information like hop distance
    and the output column being queried.
    """

    # Fields from GraphNode
    identifier: str = Field(
        ..., description="Unique node identifier (fully-qualified column name)"
    )
    file_path: str = Field(
        ..., description="Source SQL file path where first encountered"
    )
    query_index: int = Field(..., description="Index of query within the file")
    schema_name: Optional[str] = Field(None, description="Schema name (if present)")
    table: Optional[str] = Field(None, description="Table name")
    column: Optional[str] = Field(None, description="Column name")

    # Query result fields
    hops: int = Field(..., description="Number of hops from the queried column")
    output_column: str = Field(..., description="The column that was queried")

    # Path tracking and root/leaf detection fields
    is_root: bool = Field(
        default=False, description="True if node has no upstream dependencies"
    )
    is_leaf: bool = Field(
        default=False, description="True if node has no downstream dependencies"
    )
    paths: List[LineagePath] = Field(
        default_factory=list,
        description="All paths from this node to the queried column",
    )

    @classmethod
    def from_graph_node(
        cls,
        node: "GraphNode",
        hops: int,
        output_column: str,
        is_root: bool = False,
        is_leaf: bool = False,
        paths: Optional[List[LineagePath]] = None,
    ) -> "LineageNode":
        """
        Create a LineageNode from a GraphNode with additional context.

        Args:
            node: The underlying GraphNode
            hops: Number of hops from the query column
            output_column: The column that was queried
            is_root: True if node has no upstream dependencies
            is_leaf: True if node has no downstream dependencies
            paths: List of all paths from this node to the queried column

        Returns:
            LineageNode with all GraphNode fields plus query context
        """
        return cls(
            identifier=node.identifier,
            file_path=node.file_path,
            query_index=node.query_index,
            schema_name=node.schema_name,
            table=node.table,
            column=node.column,
            hops=hops,
            output_column=output_column,
            is_root=is_root,
            is_leaf=is_leaf,
            paths=paths or [],
        )


class GraphMetadata(BaseModel):
    """Metadata about the lineage graph."""

    node_format: NodeFormat = Field(
        default=NodeFormat.QUALIFIED,
        description="Format of node identifiers in serialized output",
    )
    default_dialect: str = Field(
        default="spark", description="Default SQL dialect used"
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp of graph creation",
    )
    source_files: List[str] = Field(
        default_factory=list,
        description="List of source SQL files included in the graph",
    )
    total_nodes: int = Field(
        default=0, description="Total number of nodes in the graph"
    )
    total_edges: int = Field(
        default=0, description="Total number of edges in the graph"
    )


class LineageGraph(BaseModel):
    """Serializable representation of the complete lineage graph."""

    metadata: GraphMetadata = Field(default_factory=GraphMetadata)
    nodes: List[GraphNode] = Field(
        default_factory=list, description="All nodes in the graph"
    )
    edges: List[GraphEdge] = Field(
        default_factory=list, description="All edges in the graph"
    )

    def get_node_by_identifier(self, identifier: str) -> Optional[GraphNode]:
        """
        Find a node by its identifier (case-insensitive).

        Args:
            identifier: Node identifier to find

        Returns:
            GraphNode if found, None otherwise
        """
        identifier_lower = identifier.lower()
        for node in self.nodes:
            if node.identifier.lower() == identifier_lower:
                return node
        return None
