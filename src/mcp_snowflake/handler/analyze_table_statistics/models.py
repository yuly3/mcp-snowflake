from collections.abc import Awaitable, Sequence
from typing import Literal, NotRequired, Protocol, TypedDict

import attrs
from attrs import validators
from pydantic import BaseModel, Field

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import DataBase, Schema, Table, TableColumn

from ..describe_table import EffectDescribeTable


class StatisticsResultParseError(Exception):
    """Exception raised when statistics result parsing fails.

    This exception is raised when:
    - Required keys are missing from the statistics result row
    - JSON parsing fails for TOP_VALUES fields
    - TOP_VALUES elements have invalid structure or values
    - Unexpected data types or values are encountered
    """


@attrs.define(frozen=True, slots=True)
class TopValue[T]:
    """Type-safe representation of top value with count from APPROX_TOP_K."""

    value: T | None
    count: int = attrs.field(validator=[validators.ge(0)])


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
    top_values: list[TopValue[str]]
    """List of TopValue instances with value and count pairs"""


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


class AnalyzeTableStatisticsArgs(BaseModel):
    """Arguments for analyzing table statistics."""

    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")
    columns: list[str] = Field(default_factory=list)
    """Empty list means all columns"""
    top_k_limit: int = Field(default=10, ge=1, le=100)
    """Number of most frequent values to retrieve"""


@attrs.define(frozen=True, slots=True)
class TableStatisticsParseResult:
    """Result of parsing table statistics query result row.

    Contains both the total row count and column-wise statistics
    parsed from a single database result row.
    """

    total_rows: int
    column_statistics: dict[str, StatsDict]


class EffectAnalyzeTableStatistics(EffectDescribeTable, Protocol):
    """Protocol for dependencies required by table statistics analysis."""

    def analyze_table_statistics(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_analyze: Sequence[StatisticsSupportColumn],
        top_k_limit: int,
    ) -> Awaitable[TableStatisticsParseResult]:
        """Execute statistics query and return the parsed statistics result.

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
        Awaitable[TableStatisticsParseResult]
            Parsed statistics containing total_rows and column statistics

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
        StatisticsResultParseError
            If the statistics result parsing fails
        """
        ...


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


@attrs.define(frozen=True, slots=True)
class ColumnDoesNotExist:
    """Error result when some requested columns don't exist in the table.

    Attributes
    ----------
    existed_columns : list[TableColumn]
        Columns that exist in the table among the requested ones.
    not_existed_columns : list[str]
        Column names that don't exist in the table.
    """

    existed_columns: list[TableColumn]
    not_existed_columns: list[str]


@attrs.define(frozen=True, slots=True)
class NoSupportedColumns:
    """Result when no columns support statistics analysis.

    Attributes
    ----------
    unsupported_columns : list[TableColumn]
        All columns that don't support statistics analysis.
    """

    unsupported_columns: list[TableColumn]
