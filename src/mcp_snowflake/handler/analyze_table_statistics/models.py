"""Public API models for table statistics analysis."""

from collections.abc import Iterable
from typing import Any, Protocol

from pydantic import BaseModel, Field

from ...kernel.table_metadata import TableColumn
from ..describe_table import EffectDescribeTable


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: str
    schema_name: str
    table_name: str
    columns: list[str] = Field(default_factory=list)
    """Empty list means all columns"""
    top_k_limit: int = Field(default=10, ge=1, le=100)
    """Number of most frequent values to retrieve"""


class EffectAnalyzeTableStatistics(EffectDescribeTable, Protocol):
    """Protocol for dependencies required by table statistics analysis."""

    async def analyze_table_statistics(
        self,
        database: str,
        schema_name: str,
        table_name: str,
        columns_to_analyze: Iterable[TableColumn],
        top_k_limit: int,
    ) -> dict[str, Any]:
        """Execute statistics query and return the single result row.

        Parameters
        ----------
        database : str
            Database name
        schema_name : str
            Schema name
        table_name : str
            Table name
        columns_to_analyze : Iterable[TableColumn]
            Column information objects
        top_k_limit : int
            Limit for APPROX_TOP_K function

        Returns
        -------
        dict[str, Any]
            Single row of statistics query results

        Raises
        ------
        Exception
            If query execution fails or returns no data
        """
        ...
