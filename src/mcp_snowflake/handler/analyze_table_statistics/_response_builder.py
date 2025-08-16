"""Response building utilities for table statistics analysis."""

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import mcp.types as types

from ...kernel.statistics_support_column import StatisticsSupportColumn
from ...kernel.table_metadata import TableColumn
from ._result_parser import parse_statistics_result
from ._types import AnalyzeTableStatisticsJsonResponse
from .models import AnalyzeTableStatisticsArgs

if TYPE_CHECKING:
    from ._types import AnalyzeTableStatisticsJsonResponse


def build_response(
    args: AnalyzeTableStatisticsArgs,
    result_row: Mapping[str, Any],
    columns_to_analyze: Sequence[StatisticsSupportColumn],
    unsupported_columns: Sequence[TableColumn] = (),
) -> list[types.Content]:
    """Build the final response content.

    Parameters
    ----------
    args : AnalyzeTableStatisticsArgs
        The original request arguments.
    result_row : Mapping[str, Any]
        The query result row containing statistics.
    columns_to_analyze : Sequence[TableColumn]
        The columns that were analyzed.
    unsupported_columns : Sequence[TableColumn], optional
        The columns that were not analyzed due to unsupported data types.

    Returns
    -------
    list[types.Content]
        The formatted response content.
    """
    column_statistics = parse_statistics_result(result_row, columns_to_analyze)

    response: AnalyzeTableStatisticsJsonResponse = {
        "table_statistics": {
            "table_info": {
                "database": args.database,
                "schema": args.schema_,
                "table": args.table_,
                "total_rows": result_row["TOTAL_ROWS"],  # Use uppercase for Snowflake
                "analyzed_columns": len(columns_to_analyze),
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

    numeric_count = 0
    string_count = 0
    date_count = 0
    boolean_count = 0
    for stats in column_statistics.values():
        match stats["column_type"]:
            case "numeric":
                numeric_count += 1
            case "string":
                string_count += 1
            case "date":
                date_count += 1
            case "boolean":
                boolean_count += 1

    summary_lines = [
        f"Table Statistics Analysis: {args.database}.{args.schema_}.{args.table_}",
        "",
        f"Successfully analyzed {len(columns_to_analyze)} columns with {result_row['TOTAL_ROWS']:,} total rows.",
        "",
        "**Column Types Analyzed:**",
        f"- Numeric: {numeric_count} columns",
        f"- String: {string_count} columns",
        f"- Date: {date_count} columns",
        f"- Boolean: {boolean_count} columns",
    ]

    # Add unsupported columns note if any exist
    if unsupported_columns:
        summary_lines.extend(
            [
                "",
                f"Note: Some columns were not analyzed due to unsupported data types. {len(unsupported_columns)} column(s) skipped.",
            ]
        )

    summary_lines.extend(
        [
            "",
            "Full statistical details are provided in the JSON response below.",
        ]
    )

    summary_text = "\n".join(summary_lines)

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
