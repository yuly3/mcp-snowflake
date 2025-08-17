"""Table statistics analysis module."""

import logging

from ._column_analysis import select_and_classify_columns
from ._result_parser import parse_statistics_result
from ._types import ClassifiedColumns, ColumnDoesNotExist
from .models import (
    AnalyzeTableStatisticsArgs,
    AnalyzeTableStatisticsJsonResponse,
    EffectAnalyzeTableStatistics,
)

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "AnalyzeTableStatisticsArgs",
    "EffectAnalyzeTableStatistics",
    "handle_analyze_table_statistics",
]


async def handle_analyze_table_statistics(
    args: AnalyzeTableStatisticsArgs,
    effect: EffectAnalyzeTableStatistics,
) -> AnalyzeTableStatisticsJsonResponse | ColumnDoesNotExist:
    """Handle table statistics analysis request.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The request arguments.
    effect : EffectAnalyzeTableStatistics
        Effect implementation for database operations.

    Returns
    -------
    AnalyzeTableStatisticsJsonResponse | ColumnDoesNotExist
        The structured response containing table statistics, or error information
        if requested columns don't exist.

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
        case ClassifiedColumns(
            supported_columns=supported_columns,
            unsupported_columns=unsupported_columns,
        ):
            pass

    # If no supported columns, return error as ColumnDoesNotExist
    if not supported_columns:
        return ColumnDoesNotExist(
            existed_columns=list(unsupported_columns),
            not_existed_columns=[],
        )

    result_row = await effect.analyze_table_statistics(
        args.database,
        args.schema_,
        args.table_,
        supported_columns,
        args.top_k_limit,
    )

    # Parse statistics and build structured response
    column_statistics = parse_statistics_result(result_row, supported_columns)

    response: AnalyzeTableStatisticsJsonResponse = {
        "table_statistics": {
            "table_info": {
                "database": args.database,
                "schema": args.schema_,
                "table": args.table_,
                "total_rows": result_row["TOTAL_ROWS"],
                "analyzed_columns": len(supported_columns),
            },
            "column_statistics": column_statistics,
        }
    }

    # Add unsupported_columns if any exist
    if unsupported_columns:
        response["table_statistics"]["unsupported_columns"] = [
            {"name": col.name, "data_type": col.data_type.raw_type}
            for col in unsupported_columns
        ]

    return response
