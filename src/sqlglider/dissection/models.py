"""Pydantic models for SQL dissection results."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class ComponentType(str, Enum):
    """Type of SQL component extracted from a query."""

    CTE = "CTE"
    MAIN_QUERY = "MAIN_QUERY"
    SUBQUERY = "SUBQUERY"
    SCALAR_SUBQUERY = "SCALAR_SUBQUERY"
    TARGET_TABLE = "TARGET_TABLE"
    SOURCE_QUERY = "SOURCE_QUERY"
    UNION_BRANCH = "UNION_BRANCH"


class SQLComponent(BaseModel):
    """Represents a single SQL component extracted from a query."""

    component_type: ComponentType = Field(
        ..., description="Type of component (CTE, SUBQUERY, etc.)"
    )
    component_index: int = Field(
        ..., description="0-based index within query (order of extraction)"
    )
    name: Optional[str] = Field(
        None,
        description="Name/alias of component (CTE name, subquery alias, target table)",
    )
    sql: str = Field(
        ..., description="Extracted SQL for this component (executable if applicable)"
    )
    parent_index: Optional[int] = Field(
        None, description="Index of parent component (for nested subqueries)"
    )
    depth: int = Field(
        default=0, description="Nesting depth (0 = top-level, 1+ = nested)"
    )
    is_executable: bool = Field(
        default=True, description="Whether this SQL can be executed standalone"
    )
    dependencies: List[str] = Field(
        default_factory=list,
        description="Names of CTEs this component depends on",
    )
    location: str = Field(
        ..., description="Human-readable location context (e.g., 'WITH clause')"
    )


class QueryMetadata(BaseModel):
    """Metadata about a dissected query."""

    query_index: int = Field(..., description="0-based query index in multi-query file")
    query_preview: str = Field(..., description="First 100 chars of original query")
    statement_type: str = Field(
        ..., description="Type of SQL statement (SELECT, INSERT, CREATE, etc.)"
    )
    total_components: int = Field(
        ..., description="Total number of components extracted"
    )


class QueryDissectionResult(BaseModel):
    """Complete dissection result for a single query."""

    metadata: QueryMetadata
    components: List[SQLComponent] = Field(
        default_factory=list,
        description="All extracted components in order",
    )
    original_sql: str = Field(
        ..., description="Original SQL query for reference/validation"
    )

    def get_component_by_name(self, name: str) -> Optional[SQLComponent]:
        """Find a component by name (case-insensitive).

        Args:
            name: The component name to search for.

        Returns:
            The matching SQLComponent or None if not found.
        """
        name_lower = name.lower()
        for component in self.components:
            if component.name and component.name.lower() == name_lower:
                return component
        return None

    def get_components_by_type(self, comp_type: ComponentType) -> List[SQLComponent]:
        """Get all components of a specific type.

        Args:
            comp_type: The ComponentType to filter by.

        Returns:
            List of matching SQLComponent objects.
        """
        return [c for c in self.components if c.component_type == comp_type]

    def get_executable_components(self) -> List[SQLComponent]:
        """Get all executable components.

        Returns:
            List of SQLComponent objects that can be executed standalone.
        """
        return [c for c in self.components if c.is_executable]
