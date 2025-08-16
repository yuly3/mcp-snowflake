"""Public API models for t    async def analyz        schema : str
        Schema name containing the table
    table : str
        Name of the table to analyzeble_statistics(
    database: str,
    schema: str,
    table: str,
    columns: list[StatisticsSupportColumn],
    top_k_limit: int,
) -> dict[str, Any]: ...tistics analysis."""

from collections.abc import Iterable
from typing import Any, Protocol

from pydantic import BaseModel, Field

from ...kernel.statistics_support_column import StatisticsSupportColumn
from ..describe_table import EffectDescribeTable


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: str
    schema_: str = Field(alias="schema")
    table_: str = Field(alias="table")
    columns: list[str] = Field(default_factory=list)
    """Empty list means all columns"""
    top_k_limit: int = Field(default=10, ge=1, le=100)
    """Number of most frequent values to retrieve"""


class EffectAnalyzeTableStatistics(EffectDescribeTable, Protocol):
    """Protocol for dependencies required by table statistics analysis."""

    async def analyze_table_statistics(
        self,
        database: str,
        schema: str,
        table: str,
        columns_to_analyze: Iterable[StatisticsSupportColumn],
        top_k_limit: int,
    ) -> dict[str, Any]:
        """Execute statistics query and return the single result row.

        Parameters
        ----------
        database : str
            Database name
        schema : str
            Schema name
        table : str
            Table name
        columns_to_analyze : Iterable[StatisticsSupportColumn]
            Column information objects with statistics support
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
