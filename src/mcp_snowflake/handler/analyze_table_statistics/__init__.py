"""Table statistics analysis module."""

import logging

from ._column_analysis import select_and_classify_columns
from ._serializer import (
    AnalyzeTableStatisticsResult,
    AnalyzeTableStatisticsResultSerializer,
    CompactAnalyzeTableStatisticsResultSerializer,
    UnsupportedColumnInfo,
)
from ._types import ClassifiedColumns
from .models import (
    AnalyzeTableStatisticsArgs,
    ColumnDoesNotExist,
    EffectAnalyzeTableStatistics,
    NoSupportedColumns,
    StatisticsResultParseError,
    TableStatisticsParseResult,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "AnalyzeTableStatisticsArgs",
    "AnalyzeTableStatisticsResult",
    "AnalyzeTableStatisticsResultSerializer",
    "ColumnDoesNotExist",
    "CompactAnalyzeTableStatisticsResultSerializer",
    "EffectAnalyzeTableStatistics",
    "NoSupportedColumns",
    "StatisticsResultParseError",
    "TableStatisticsParseResult",
    "handle_analyze_table_statistics",
]


async def handle_analyze_table_statistics(
    args: AnalyzeTableStatisticsArgs,
    effect: EffectAnalyzeTableStatistics,
) -> AnalyzeTableStatisticsResult | ColumnDoesNotExist | NoSupportedColumns:
    """Handle table statistics analysis request.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The request arguments.
    effect : EffectAnalyzeTableStatistics
        Effect implementation for database operations.

    Returns
    -------
    AnalyzeTableStatisticsResult | ColumnDoesNotExist | NoSupportedColumns
        The structured result containing table statistics, error information
        if requested columns don't exist, or NoSupportedColumns if no columns
        support statistics analysis.

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
    table_info = await effect.describe_table(
        args.database,
        args.schema_,
        args.table_,
    )

    classified_columns = select_and_classify_columns(
        table_info.columns,
        args.columns,
    )
    match classified_columns:
        case ColumnDoesNotExist() as column_error:
            return column_error
        case NoSupportedColumns() as no_supported:
            return no_supported
        case ClassifiedColumns(
            supported_columns=supported_columns,
            unsupported_columns=unsupported_columns,
        ):
            pass

    # Continue with supported columns processing

    parsed = await effect.analyze_table_statistics(
        args.database,
        args.schema_,
        args.table_,
        supported_columns,
        args.top_k_limit,
        include_null_empty_profile=args.include_null_empty_profile,
        include_blank_string_profile=args.include_blank_string_profile,
    )

    unsupported_info = [
        UnsupportedColumnInfo(name=col.name, data_type=col.data_type.raw_type) for col in unsupported_columns
    ]

    return AnalyzeTableStatisticsResult(
        database=args.database,
        schema=args.schema_,
        table=args.table_,
        total_rows=parsed.total_rows,
        column_statistics=parsed.column_statistics,
        unsupported_columns=unsupported_info,
        include_statistics_metadata=args.include_null_empty_profile,
    )
