"""Internal type definitions for table statistics analysis."""

from typing import Any, Literal, TypedDict


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


class AnalyzeTableStatisticsJsonResponse(TypedDict):
    """TypedDict for the complete JSON response structure."""

    table_statistics: TableStatisticsDict
