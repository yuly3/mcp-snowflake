"""Table statistics analysis module."""

import logging

from ._column_analysis import select_and_classify_columns
from ._types import ClassifiedColumns
from .models import (
    AnalyzeTableStatisticsArgs,
    AnalyzeTableStatisticsJsonResponse,
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
    "AnalyzeTableStatisticsJsonResponse",
    "ColumnDoesNotExist",
    "EffectAnalyzeTableStatistics",
    "NoSupportedColumns",
    "StatisticsResultParseError",
    "TableStatisticsParseResult",
    "handle_analyze_table_statistics",
]


async def handle_analyze_table_statistics(
    args: AnalyzeTableStatisticsArgs,
    effect: EffectAnalyzeTableStatistics,
) -> AnalyzeTableStatisticsJsonResponse | ColumnDoesNotExist | NoSupportedColumns:
    """Handle table statistics analysis request.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The request arguments.
    effect : EffectAnalyzeTableStatistics
        Effect implementation for database operations.

    Returns
    -------
    AnalyzeTableStatisticsJsonResponse | ColumnDoesNotExist | NoSupportedColumns
        The structured response containing table statistics, error information
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

    # Build structured response using parsed result
    response: AnalyzeTableStatisticsJsonResponse = {
        "table_statistics": {
            "table_info": {
                "database": args.database,
                "schema": args.schema_,
                "table": args.table_,
                "total_rows": parsed.total_rows,
                "analyzed_columns": len(supported_columns),
            },
            "column_statistics": parsed.column_statistics,
        }
    }

    if args.include_null_empty_profile:
        response["table_statistics"]["statistics_metadata"] = {
            "quality_profile_counting_mode": "exact",
            "distribution_metrics_mode": "approximate",
        }

    # Add unsupported_columns if any exist
    if unsupported_columns:
        response["table_statistics"]["unsupported_columns"] = [
            {"name": col.name, "data_type": col.data_type.raw_type} for col in unsupported_columns
        ]

    return response
