from collections.abc import Awaitable, Sequence
from typing import Any, Literal, NotRequired, Protocol, TypedDict

from pydantic import BaseModel, Field

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table

from ..describe_table import EffectDescribeTable


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")
    columns: list[str] = Field(default_factory=list)
    """Empty list means all columns"""
    top_k_limit: int = Field(default=10, ge=1, le=100)
    """Number of most frequent values to retrieve"""


class EffectAnalyzeTableStatistics(EffectDescribeTable, Protocol):
    """Protocol for dependencies required by table statistics analysis."""

    def analyze_table_statistics(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_analyze: Sequence[StatisticsSupportColumn],
        top_k_limit: int,
    ) -> Awaitable[dict[str, Any]]:
        """Execute statistics query and return the single result row.

        Parameters
        ----------
        database : DataBase
            Database name
        schema : Schema
            Schema name
        table : Table
            Table name
        columns_to_analyze : Sequence[StatisticsSupportColumn]
            Column information objects with statistics support
        top_k_limit : int
            Limit for APPROX_TOP_K function

        Returns
        -------
        Awaitable[dict[str, Any]]
            Single row of statistics query results

        Raises
        ------
        TimeoutError
            If query execution times out
        ProgrammingError
            SQL syntax errors or other programming errors
        OperationalError
            Database operation related errors
        DataError
            Data processing related errors
        IntegrityError
            Referential integrity constraint violations
        NotSupportedError
            When an unsupported database feature is used
        """
        ...


class NumericStatsDict(TypedDict):
    """TypedDict for numeric column statistics."""

    column_type: Literal["numeric"]
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min: float
    max: float
    avg: float
    percentile_25: float
    percentile_50: float  # median
    percentile_75: float


class StringStatsDict(TypedDict):
    """TypedDict for string column statistics."""

    column_type: Literal["string"]
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_length: int
    max_length: int
    top_values: list[list[Any]]
    """[[value, count], ...]"""


class DateStatsDict(TypedDict):
    """TypedDict for date column statistics."""

    column_type: Literal["date"]
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_date: str
    max_date: str
    date_range_days: int


class BooleanStatsDict(TypedDict):
    """TypedDict for boolean column statistics."""

    column_type: Literal["boolean"]
    data_type: str
    count: int
    null_count: int
    true_count: int
    false_count: int
    true_percentage: float
    """Excludes NULL values (DIV0NULL returns 0.00)"""
    false_percentage: float
    """Excludes NULL values (DIV0NULL returns 0.00)"""
    true_percentage_with_nulls: float
    """Includes NULL values"""
    false_percentage_with_nulls: float
    """Includes NULL values"""


StatsDict = NumericStatsDict | StringStatsDict | DateStatsDict | BooleanStatsDict


class UnsupportedColumnDict(TypedDict):
    """TypedDict for unsupported column information."""

    name: str
    data_type: str


class TableInfoDict(TypedDict):
    """TypedDict for table information."""

    database: str
    schema: str
    table: str
    total_rows: int
    analyzed_columns: int


class TableStatisticsDict(TypedDict):
    """TypedDict for the complete table statistics response."""

    table_info: TableInfoDict
    column_statistics: dict[str, StatsDict]
    unsupported_columns: NotRequired[list[UnsupportedColumnDict]]


class AnalyzeTableStatisticsJsonResponse(TypedDict):
    """TypedDict for the complete JSON response structure."""

    table_statistics: TableStatisticsDict
