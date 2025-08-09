"""Response building utilities for table statistics analysis."""

import json
from typing import TYPE_CHECKING, Any

import mcp.types as types

from ._result_parser import parse_statistics_result
from ._types import ColumnInfo
from .models import AnalyzeTableStatisticsArgs

if TYPE_CHECKING:
    from ._types import AnalyzeTableStatisticsJsonResponse


def build_response(
    args: AnalyzeTableStatisticsArgs,
    result_row: dict[str, Any],
    columns_to_analyze: list[ColumnInfo],
) -> list[types.Content]:
    """Build the final response content.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The original request arguments.
    result_row : dict[str, Any]
        The query result row containing statistics.
    columns_to_analyze : list[ColumnInfo]
        The columns that were analyzed.

    Returns
    -------
    list[types.Content]
        The formatted response content.
    """
    # Parse results into structured format
    column_statistics = parse_statistics_result(result_row, columns_to_analyze)

    # Build response
    response: AnalyzeTableStatisticsJsonResponse = {
        "table_statistics": {
            "table_info": {
                "database": args.database,
                "schema": args.schema_name,
                "table": args.table_name,
                "total_rows": result_row["TOTAL_ROWS"],  # Use uppercase for Snowflake
                "analyzed_columns": len(columns_to_analyze),
            },
            "column_statistics": column_statistics,
        }
    }

    numeric_count = sum(
        1 for stats in column_statistics.values() if stats["column_type"] == "numeric"
    )
    string_count = sum(
        1 for stats in column_statistics.values() if stats["column_type"] == "string"
    )
    date_count = sum(
        1 for stats in column_statistics.values() if stats["column_type"] == "date"
    )
    boolean_count = sum(
        1 for stats in column_statistics.values() if stats["column_type"] == "boolean"
    )

    summary_text = "\n".join(
        [
            f"Table Statistics Analysis: {args.database}.{args.schema_name}.{args.table_name}",
            "",
            f"Successfully analyzed {len(columns_to_analyze)} columns with {result_row['TOTAL_ROWS']:,} total rows.",
            "",
            "**Column Types Analyzed:**",
            f"- Numeric: {numeric_count} columns",
            f"- String: {string_count} columns",
            f"- Date: {date_count} columns",
            f"- Boolean: {boolean_count} columns",
            "",
            "Full statistical details are provided in the JSON response below.",
        ]
    )

    return [
        types.TextContent(
            type="text",
            text=summary_text,
        ),
        types.TextContent(
            type="text",
            text=json.dumps(response, indent=2, ensure_ascii=False),
        ),
    ]
