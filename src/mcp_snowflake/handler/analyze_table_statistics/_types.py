"""Internal type definitions for table statistics analysis."""

from typing import Any, TypedDict


class NumericStatsDict(TypedDict):
    """TypedDict for numeric column statistics."""

    column_type: str  # "numeric"
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

    column_type: str  # "string"
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_length: int
    max_length: int
    top_values: list[list[Any]]  # [[value, count], ...]


class DateStatsDict(TypedDict):
    """TypedDict for date column statistics."""

    column_type: str  # "date"
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_date: str
    max_date: str
    date_range_days: int


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
    column_statistics: dict[str, NumericStatsDict | StringStatsDict | DateStatsDict]


class AnalyzeTableStatisticsJsonResponse(TypedDict):
    """TypedDict for the complete JSON response structure."""

    table_statistics: TableStatisticsDict
