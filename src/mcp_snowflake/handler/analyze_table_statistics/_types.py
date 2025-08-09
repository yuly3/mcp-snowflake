"""Internal type definitions for table statistics analysis."""

from collections.abc import Mapping
from typing import Any, TypedDict

import attrs

from mcp_snowflake.kernel import SnowflakeDataType, StatisticsSupportDataType


@attrs.define(frozen=True)
class ColumnInfo:
    """Column information for statistics analysis."""

    name: str
    snowflake_type: SnowflakeDataType
    statistics_type: StatisticsSupportDataType

    @classmethod
    def from_dict(cls, col_dict: Mapping[str, Any]) -> "ColumnInfo":
        """Convert dictionary column info to ColumnInfo.

        Parameters
        ----------
        col_dict : Mapping[str, Any]
            Dictionary containing column information with 'name' and 'data_type' keys.

        Returns
        -------
        ColumnInfo
            Column information object with type-safe data types.

        Raises
        ------
        KeyError
            If required keys ('name' or 'data_type') are missing.
        ValueError
            If the data type is not supported for statistics or is invalid.
        """
        sf_type = SnowflakeDataType(col_dict["data_type"])
        stats_type = StatisticsSupportDataType.from_snowflake_type(sf_type)
        return cls(
            name=col_dict["name"],
            snowflake_type=sf_type,
            statistics_type=stats_type,
        )

    @property
    def column_type(self) -> str:
        """Backward compatibility property.

        Returns
        -------
        str
            The column type as string: "numeric", "string", or "date".
        """
        return self.statistics_type.type_name


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


class BooleanStatsDict(TypedDict):
    """TypedDict for boolean column statistics."""

    column_type: str  # "boolean"
    data_type: str
    count: int
    null_count: int
    true_count: int
    false_count: int
    true_percentage: float  # NULL除外版（DIV0NULLで0.00になる）
    false_percentage: float  # NULL除外版（DIV0NULLで0.00になる）
    true_percentage_with_nulls: float  # NULL含む版
    false_percentage_with_nulls: float  # NULL含む版


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
