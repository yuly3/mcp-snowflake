import json
from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError

from ..handler import (
    AnalyzeTableStatisticsArgs,
    EffectAnalyzeTableStatistics,
    handle_analyze_table_statistics,
)
from ..handler.analyze_table_statistics._types import ColumnDoesNotExist
from ..handler.analyze_table_statistics.models import AnalyzeTableStatisticsJsonResponse
from .base import Tool


class AnalyzeTableStatisticsTool(Tool):
    def __init__(self, effect_handler: EffectAnalyzeTableStatistics) -> None:
        self.effect_handler = effect_handler

    @property
    def name(self) -> str:
        return "analyze_table_statistics"

    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = AnalyzeTableStatisticsArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for analyze_table_statistics: {e}",
                )
            ]

        try:
            result = await handle_analyze_table_statistics(args, self.effect_handler)
        except TimeoutError as e:
            text = f"Error: Query timed out: {e}"
        except ProgrammingError as e:
            text = f"Error: SQL syntax error or other programming error: {e}"
        except OperationalError as e:
            text = f"Error: Database operation related error: {e}"
        except DataError as e:
            text = f"Error: Data processing related error: {e}"
        except IntegrityError as e:
            text = f"Error: Referential integrity constraint violation: {e}"
        except NotSupportedError as e:
            text = f"Error: Unsupported database feature used: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            # Handle structured response or error cases
            match result:
                case ColumnDoesNotExist(not_existed_columns=not_existed_columns) if (
                    not_existed_columns
                ):
                    text = f"Error: Columns not found in table: {', '.join(not_existed_columns)}"
                case ColumnDoesNotExist(existed_columns=unsupported_columns):
                    # This means no supported columns case
                    unsupported_list = [
                        f"{col.name}({col.data_type.raw_type})"
                        for col in unsupported_columns
                    ]
                    text = f"Error: No supported columns for statistics. Unsupported columns: {', '.join(unsupported_list)}"
                case response:
                    # Successful case - build summary and JSON response
                    summary_text = _build_summary_text(response)

                    return [
                        types.TextContent(type="text", text=summary_text),
                        types.TextContent(
                            type="text",
                            text=json.dumps(response, indent=2, ensure_ascii=False),
                        ),
                    ]

        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Analyze table statistics using Snowflake's high-performance approximation functions (APPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCT) to efficiently retrieve statistical information for numeric, string, date, and boolean columns",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name containing the table",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name containing the table",
                    },
                    "table": {
                        "type": "string",
                        "description": "Name of the table to analyze",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of column names to analyze (if not specified, all columns will be analyzed)",
                        "default": [],
                    },
                    "top_k_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 10,
                        "description": "Number of top values to retrieve for string columns",
                    },
                },
                "required": ["database", "schema", "table"],
            },
        )


def _build_summary_text(response: AnalyzeTableStatisticsJsonResponse) -> str:
    """Build summary text from statistics response.

    Parameters
    ----------
    response : dict[str, Any]
        The structured statistics response

    Returns
    -------
    str
        Formatted summary text
    """
    stats = response["table_statistics"]
    table_info = stats["table_info"]
    column_stats = stats["column_statistics"]

    # Count column types
    numeric_count = sum(
        1 for s in column_stats.values() if s["column_type"] == "numeric"
    )
    string_count = sum(1 for s in column_stats.values() if s["column_type"] == "string")
    date_count = sum(1 for s in column_stats.values() if s["column_type"] == "date")
    boolean_count = sum(
        1 for s in column_stats.values() if s["column_type"] == "boolean"
    )

    summary_lines = [
        f"Table Statistics Analysis: {table_info['database']}.{table_info['schema']}.{table_info['table']}",
        "",
        f"Successfully analyzed {table_info['analyzed_columns']} columns with {table_info['total_rows']:,} total rows.",
        "",
        "**Column Types Analyzed:**",
        f"- Numeric: {numeric_count} columns",
        f"- String: {string_count} columns",
        f"- Date: {date_count} columns",
        f"- Boolean: {boolean_count} columns",
    ]

    # Add unsupported columns note if any exist
    unsupported_columns = stats.get("unsupported_columns")
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

    return "\n".join(summary_lines)
