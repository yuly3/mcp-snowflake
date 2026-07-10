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
    CompactAnalyzeTableStatisticsResultSerializer,
    EffectAnalyzeTableStatistics,
    handle_analyze_table_statistics,
)
from ..handler.analyze_table_statistics import (
    ColumnDoesNotExist,
    NoSupportedColumns,
    StatisticsResultParseError,
)
from .base import Tool


class AnalyzeTableStatisticsTool(Tool):
    def __init__(
        self,
        effect_handler: EffectAnalyzeTableStatistics,
    ) -> None:
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
            hint = (
                " Hint: Specify a subset of columns via the 'columns' parameter to reduce scan volume and avoid timeouts."
                if not args.columns
                else " Hint: Try reducing the number of columns specified in the 'columns' parameter."
            )
            text = f"Error: Query timed out: {e}:{hint}"
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
        except StatisticsResultParseError as e:
            text = f"Error: Snowflake returned unexpected result format: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            # Handle structured response or error cases
            match result:
                case ColumnDoesNotExist(not_existed_columns=not_existed_columns):
                    text = f"Error: Columns not found in table: {', '.join(not_existed_columns)}"
                case NoSupportedColumns(unsupported_columns=unsupported_columns):
                    unsupported_list = [f"{col.name}({col.data_type.raw_type})" for col in unsupported_columns]
                    text = f"Error: No supported columns for statistics. Unsupported columns: {', '.join(unsupported_list)}"
                case response:
                    text = response.serialize_with(CompactAnalyzeTableStatisticsResultSerializer())

        return [types.TextContent(type="text", text=text)]

    @property
    def definition(self) -> types.Tool:
        return types.Tool(
            name=self.name,
            description="Analyze approximate table statistics for numeric, string, date, and boolean columns.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                    },
                    "schema": {
                        "type": "string",
                    },
                    "table": {
                        "type": "string",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to analyze; empty means all.",
                        "default": [],
                    },
                    "top_k_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 10,
                        "description": "Top values for string columns.",
                    },
                    "include_null_empty_profile": {
                        "type": "boolean",
                        "default": True,
                        "description": "NULL and empty-string ratios.",
                    },
                    "include_blank_string_profile": {
                        "type": "boolean",
                        "default": False,
                        "description": "TRIM-blank ratios for strings.",
                    },
                },
                "required": ["database", "schema", "table"],
            },
        )
