"""Shared models and enums used across sqlglider modules."""

from enum import Enum


class AnalysisLevel(str, Enum):
    """Analysis granularity level for lineage."""

    COLUMN = "column"
    TABLE = "table"


class NodeFormat(str, Enum):
    """Format for node identifiers in graph output."""

    QUALIFIED = "qualified"
    STRUCTURED = "structured"
