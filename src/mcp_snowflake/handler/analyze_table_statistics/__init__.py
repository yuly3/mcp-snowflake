"""Table statistics analysis module."""

import logging
from collections.abc import Iterable
from typing import Any

import mcp.types as types

from ._column_analysis import validate_and_select_columns
from ._response_builder import build_response
from ._sql_generator import generate_statistics_sql
from ._types import ColumnInfo
from .models import AnalyzeTableStatisticsArgs, EffectAnalyzeTableStatistics

logger = logging.getLogger(__name__)

# Public API exports
__all__ = [
    "AnalyzeTableStatisticsArgs",
    "EffectAnalyzeTableStatistics",
    "handle_analyze_table_statistics",
]


async def _execute_statistics_query(
    effect: EffectAnalyzeTableStatistics,
    args: AnalyzeTableStatisticsArgs,
    columns_to_analyze: Iterable[ColumnInfo],
) -> dict[str, Any]:
    """Execute the statistics query and return the result row.

    Parameters
    ----------
    effect : EffectAnalyzeTableStatistics
        Effect implementation for database operations.
    args : AnalyzeTableStatisticsArgs
        The request arguments.
    columns_to_analyze : Iterable[ColumnInfo]
        The columns to analyze.

    Returns
    -------
    dict[str, Any]
        The query result row.

    Raises
    ------
    ValueError
        If query execution fails or returns no data.
    """
    stats_sql = generate_statistics_sql(
        args.database,
        args.schema_name,
        args.table_name,
        columns_to_analyze,
        args.top_k_limit,
    )

    query_result = await effect.execute_query(stats_sql)

    if not query_result:
        raise ValueError("No data returned from statistics query")

    return query_result[0]


async def handle_analyze_table_statistics(
    args: AnalyzeTableStatisticsArgs,
    effect: EffectAnalyzeTableStatistics,
) -> list[types.Content]:
    """Handle table statistics analysis request.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The request arguments.
    effect : EffectAnalyzeTableStatistics
        Effect implementation for database operations.

    Returns
    -------
    list[types.Content]
        The response content.
    """
    try:
        table_info = await effect.describe_table(
            args.database,
            args.schema_name,
            args.table_name,
        )
    except Exception as e:
        logger.exception("Error getting table information")
        return [
            types.TextContent(
                type="text",
                text=f"Error getting table information: {e!s}",
            )
        ]

    columns_to_analyze = validate_and_select_columns(
        table_info.columns,
        args.columns,
    )
    if isinstance(columns_to_analyze, types.TextContent):
        return [columns_to_analyze]

    try:
        result_row = await _execute_statistics_query(effect, args, columns_to_analyze)
    except Exception as e:
        logger.exception("Error executing statistics query")
        return [
            types.TextContent(
                type="text",
                text=f"Error executing statistics query: {e!s}",
            )
        ]

    try:
        return build_response(args, result_row, columns_to_analyze)
    except Exception as e:
        logger.exception("Error building response")
        return [
            types.TextContent(
                type="text",
                text=f"Error building response: {e!s}",
            )
        ]
