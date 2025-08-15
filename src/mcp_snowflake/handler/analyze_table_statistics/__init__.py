"""Table statistics analysis module."""

import logging

import mcp.types as types

from ._column_analysis import select_and_classify_columns
from ._response_builder import build_response
from .models import AnalyzeTableStatisticsArgs, EffectAnalyzeTableStatistics

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

    columns_result = select_and_classify_columns(
        table_info.columns,
        args.columns,
    )
    if isinstance(columns_result, types.TextContent):
        return [columns_result]

    supported_columns, unsupported_info = columns_result

    # If no supported columns, return error with unsupported column list
    if not supported_columns:
        unsupported_list = [
            f"{col.name}({col.data_type.raw_type})" for col, _ in unsupported_info
        ]
        error_text = f"Error: No supported columns for statistics. Unsupported columns: {', '.join(unsupported_list)}"
        return [
            types.TextContent(
                type="text",
                text=error_text,
            )
        ]

    try:
        result_row = await effect.analyze_table_statistics(
            args.database,
            args.schema_name,
            args.table_name,
            supported_columns,
            args.top_k_limit,
        )
    except Exception as e:
        logger.exception("Error executing statistics query")
        return [
            types.TextContent(
                type="text",
                text=f"Error executing statistics query: {e!s}",
            )
        ]

    # Extract unsupported columns from unsupported_info for response builder
    unsupported_columns = [col for col, _ in unsupported_info]

    try:
        return build_response(args, result_row, supported_columns, unsupported_columns)
    except Exception as e:
        logger.exception("Error building response")
        return [
            types.TextContent(
                type="text",
                text=f"Error building response: {e!s}",
            )
        ]
